import pytest

from ggp_client import client


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(client, "RETRY_WAIT", 0)  # retries without the pauses
