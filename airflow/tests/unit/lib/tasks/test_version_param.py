import pytest
from airflow.sdk.definitions.param import ParamValidationError

from opendatalake.lib.tasks import version_param


def test_version_param_is_free_text_without_supported_versions():
    param = version_param()["version"]
    assert param.value is None
    assert "enum" not in param.schema
    assert param.resolve("anything") == "anything"


def test_version_param_is_a_drop_down_of_supported_versions():
    param = version_param(("4.1", "4.1.2"))["version"]
    # Airflow renders a string param with an `enum` as a select list.
    assert param.schema["enum"] == ["4.1", "4.1.2"]
    assert param.schema["type"] == "string"
    assert param.value == "4.1.2"
    assert param.resolve("4.1") == "4.1"


def test_version_param_rejects_an_unsupported_version():
    param = version_param(("4.1.2",))["version"]
    with pytest.raises(ParamValidationError):
        param.resolve("4.1.1")
