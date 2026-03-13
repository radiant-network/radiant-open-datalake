from dags.lib.utils.humanize import bytes_to_human_readable
import pytest


@pytest.mark.parametrize(
    "input_bytes,expected",
    [
        (0, "0.00 Bytes"),
        (512, "512.00 Bytes"),
        (1024, "1.00 KB"),
        (1024 * 1024, "1.00 MB"),
        (1024 * 1024 * 1024, "1.00 GB"),
        (1024 ** 4, "1.00 TB"),
        (1024 ** 5, "1.00 PB"),
        (1024 ** 5 * 1.5, "1.50 PB"),
    ]
)
def test_bytes_to_human_readable(input_bytes, expected):
    assert bytes_to_human_readable(input_bytes) == expected
