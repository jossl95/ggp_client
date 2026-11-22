"""The filters that choose datasets: one list, used by the command line and the client."""
import sys

from . import db

FIELDS = ("project", "round", "wave", "country", "ids", "all_versions")  # the arguments of db.select


def add_arguments(ap):
    """The filter options of the command line."""
    ap.add_argument("--project", nargs="+", default=[], help="ggs, ffs, hh, other, teaching (default: all)")
    ap.add_argument("--round", nargs="+", default=[], help="1 or 2, or i, ii (default: all)")
    ap.add_argument("--wave", nargs="+", default=[], help="w1 w2 pilot ... (default: all)")
    ap.add_argument("--country", nargs="+", default=[], help="country codes, e.g. nl at (default: all)")
    ap.add_argument("--ids", help="catalogue ids or ranges, e.g. 3-12,20")
    ap.add_argument("--all-versions", action="store_true", help="include superseded versions")


def from_args(a):
    """The filters of parsed command line arguments, as keyword arguments of ggpClient or db.select."""
    try:
        ids = db.parse_ids(a.ids) if a.ids else None
    except ValueError as e:
        sys.exit(str(e))
    return dict(project=a.project, round=a.round, wave=a.wave, country=a.country, ids=ids,
                all_versions=a.all_versions)
