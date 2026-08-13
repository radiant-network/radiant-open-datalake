from unittest.mock import MagicMock, patch

import pytest

from opendatalake.lib.tasks import get_version


class DummyAsset:
    def __init__(self, uri):
        self.uri = uri

    # Implementing __hash__ and __eq__ so DummyAsset can be used as a dict key in tests.
    # Note: In this test setup, using real Airflow Asset objects as dict keys does not work as expected.
    # This is likely due to additional logic or metaclass behavior in Airflow’s Asset implementation.
    def __hash__(self):
        return hash(self.uri)

    def __eq__(self, other):
        return isinstance(other, DummyAsset) and self.uri == other.uri


@pytest.fixture(autouse=True)
def patch_task_decorator():
    with patch("opendatalake.lib.tasks.task", lambda *a, **k: lambda f: f):
        yield


def test_get_version_single_event():
    asset = DummyAsset("test-asset")
    event = MagicMock(extra={"version": "v1.2.3"})
    triggering_asset_events = {asset: [event]}
    assert get_version(asset, triggering_asset_events=triggering_asset_events) == "v1.2.3"


def test_get_version_missing_asset():
    asset = DummyAsset("test-asset")
    with pytest.raises(KeyError):
        get_version(asset, triggering_asset_events={})


def test_get_version_no_events():
    asset = DummyAsset("test-asset")
    with pytest.raises(ValueError, match="No triggering events found"):
        get_version(asset, triggering_asset_events={asset: []})


def test_get_version_manual_param_fallback():
    asset = DummyAsset("test-asset")
    result = get_version(asset, triggering_asset_events={asset: []}, params={"version": "manual-v9"})
    assert result == "manual-v9"


def test_get_version_no_events_no_param_raises():
    asset = DummyAsset("test-asset")
    with pytest.raises(ValueError, match="no 'version' param supplied"):
        get_version(asset, triggering_asset_events={asset: []}, params={})


def test_get_version_inactive_asset_reads_param_only():
    # MANUAL source: inactive input asset -> no inlet, version read solely from the param.
    asset = DummyAsset("test-asset")
    assert get_version(asset, asset_active=False, params={"version": "4.9a"}) == "4.9a"


def test_get_version_inactive_asset_no_param_raises():
    asset = DummyAsset("test-asset")
    with pytest.raises(ValueError, match="'version' param"):
        get_version(asset, asset_active=False, params={})


def test_get_version_asset_event_precedes_param():
    asset = DummyAsset("test-asset")
    event = MagicMock(extra={"version": "from-asset"})
    result = get_version(asset, triggering_asset_events={asset: [event]}, params={"version": "from-param"})
    assert result == "from-asset"


def test_get_version_multiple_events_warns():
    asset = DummyAsset("test-asset")
    triggering_asset_events = {asset: [MagicMock(extra={"version": "v1.0.0"}), MagicMock(extra={"version": "v2.0.0"})]}

    with patch("logging.warning") as mock_warn:
        result = get_version(asset, triggering_asset_events=triggering_asset_events)
        assert result == "v2.0.0"
        mock_warn.assert_called_once()


def test_get_version_missing_version_in_event():
    asset = DummyAsset("test-asset")
    event = MagicMock(extra={})
    with pytest.raises(KeyError):
        get_version(asset, triggering_asset_events={asset: [event]})
