"""Lower-case variable names after download, and putting labels on the data."""
import json

import pandas as pd
import polars as pl
import pyreadstat
import pytest

from ggp_client import labels, processing

ROW = dict(project="GGS", round=2, series="GGS-II", wave="w1", country="AT", version="1", local_path="at")


def test_preprocess_writes_lower_case_names(tmp_path):
    root, out = tmp_path / "raw", tmp_path / "processed"
    (root / "at").mkdir(parents=True)
    pyreadstat.write_dta(pd.DataFrame({"DEM01": [1.0, 2.0], "Age": [30.0, 40.0]}), root / "at" / "AT.dta",
                         column_labels=["Gender", "Age"], variable_value_labels={"DEM01": {1: "Male", 2: "Female"}})
    assert processing.preprocess([ROW], root, out) == (1, 0, [])
    df = pl.read_parquet(out / "at" / "AT.parquet")
    assert df.columns[:2] == ["dem01", "age"] and df["dem01"].to_list() == [1, 2]
    m = json.loads((out / "at" / "AT.meta.json").read_text())
    assert m["column_labels"] == {"dem01": "Gender", "age": "Age"}
    assert m["value_labels"] == {"dem01": {"1": "Male", "2": "Female"}} and set(m["original_types"]) == {"dem01", "age"}
    assert processing.preprocess([ROW], root, out) == (0, 1, [])  # lower case already: kept
    df.rename({"dem01": "DEM01"}).write_parquet(out / "at" / "AT.parquet")  # as an older version wrote it
    assert processing.preprocess([ROW], root, out) == (1, 0, [])  # upper-case names: made again
    assert pl.read_parquet(out / "at" / "AT.parquet").columns[0] == "dem01"


def test_names_that_differ_only_in_case_are_an_error():
    assert processing.lower_names(["A", "b"], "x") == {"A": "a", "b": "b"}
    with pytest.raises(ValueError, match="x: V1 and v1 differ only in case"):
        processing.lower_names(["V1", "v1"], "x")


META = {"column_labels": {"edu": "Education", "sex": "Sex", "id": None},
        "value_labels": {"edu": {"1": "Elementary school", "2": "Elementary school", "3": "University", "a": "Refused"},
                         "sex": {"M": "Male", "F": "Female"}}}


def test_label_puts_value_labels_on_a_polars_frame():
    df = pl.DataFrame({"id": [1, 2, 3, 4], "edu": [3.0, 1.0, None, 7.0], "sex": ["F", "M", "F", None]})
    out = labels.label(df, META)
    assert out["edu"].to_list() == ["University", "Elementary school", None, "7"]  # a code without a label: itself
    assert out["edu"].dtype == pl.Enum(["Elementary school", "University", "7"])  # labels in code order, shared once
    assert out["sex"].to_list() == ["Female", "Male", "Female", None]
    assert out["id"].to_list() == [1, 2, 3, 4]  # no value labels: left alone
    assert labels.label(df, META, columns=["sex"])["edu"].to_list() == [3.0, 1.0, None, 7.0]
    assert labels.variable_labels(out, META) == {"edu": "Education", "sex": "Sex"}


def test_label_on_a_pandas_frame_keeps_variable_labels_in_attrs():
    out = labels.label(pd.DataFrame({"edu": [1, 3], "other": [5, 6]}), META)
    assert isinstance(out, pd.DataFrame) and list(out["edu"]) == ["Elementary school", "University"]
    assert isinstance(out["edu"].dtype, pd.CategoricalDtype) and out.attrs["column_labels"] == {"edu": "Education"}


def test_label_handles_an_all_null_column():
    out = labels.label(pl.DataFrame({"edu": [None, None]}, schema={"edu": pl.Float64}), META)
    assert out["edu"].to_list() == [None, None] and out["edu"].dtype == pl.Enum(["Elementary school", "University"])


def test_meta_reads_the_meta_json_next_to_a_parquet(tmp_path):
    pq = tmp_path / "x.parquet"
    pl.DataFrame({"edu": [1]}).write_parquet(pq)
    assert labels.meta(pq) is None  # no meta.json
    labels.meta_path(pq).write_text(json.dumps(META))
    assert labels.meta(pq) == META
    assert labels.label(pl.read_parquet(pq), labels.meta(pq))["edu"].to_list() == ["Elementary school"]
