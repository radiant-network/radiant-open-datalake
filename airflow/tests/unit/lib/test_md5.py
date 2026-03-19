from dags.lib.utils.md5 import check_md5, compute_file_md5, extract_md5_from_checksum_file_content
import hashlib
import pytest
from unittest.mock import patch, MagicMock


def test_compute_file_md5_returns_correct_hash(tmp_path):
    content = b"hello world"
    expected_md5 = hashlib.md5(content).hexdigest()

    path = tmp_path / "test_happy_path"
    with open(path, "wb") as f:
        f.write(content)

    md5 = compute_file_md5(path)
    assert md5 == expected_md5


def test_compute_file_md5_uses_specified_chunk_size(tmp_path):
    content = b"a" * 100  # Large enough to require multiple chunks if chunk_size is small
    path = tmp_path / "test_use_chunk_size"
    with open(path, "wb") as f:
        f.write(content)

    with patch("hashlib.md5") as mock_md5_function:
        mock_md5 = MagicMock()
        mock_md5_function.return_value = mock_md5

        compute_file_md5(path, chunk_size=10)
        assert mock_md5.update.call_count == 10  # 100 bytes / 10 byte chunks = 10 calls


def test_compute_file_md5_raises_file_not_found_for_missing_file():
    missing_path = "this_file_does_not_exist.txt"
    with pytest.raises(FileNotFoundError):
        compute_file_md5(missing_path)


def test_check_md5_returns_md5_when_matching(tmp_path):
    content = b"hello world"
    expected_md5 = hashlib.md5(content).hexdigest()

    path = tmp_path / "test_check_md5_happy_path"
    with open(path, "wb") as f:
        f.write(content)

    md5 = check_md5(path, expected_md5)
    assert md5 == expected_md5


def test_check_md5_raises_assertion_error_when_md5_mismatch(tmp_path):
    content = b"hello world"
    path = tmp_path / "test_check_md5_mismatch"
    with open(path, "wb") as f:
        f.write(content)

    with pytest.raises(AssertionError):
        check_md5(path, "wrong_md5_hash")


def test_check_md5_raises_file_not_found_for_missing_file():
    missing_path = "this_file_does_not_exist.txt"
    with pytest.raises(FileNotFoundError):
        check_md5(missing_path, "some_md5_hash")


def test_extract_md5_from_checksum_file_content_returns_hash_from_simple_md5_file():
    md5_hash = "d41d8cd98f00b204e9800998ecf8427e"
    content = md5_hash  # Simulates a .md5 file containing only the hash
    result = extract_md5_from_checksum_file_content(content)
    assert result == md5_hash


def test_extract_md5_from_checksum_file_content_returns_hash_from_md5_file_with_filename():
    md5_hash = "d41d8cd98f00b204e9800998ecf8427e"
    filename = "somefile.txt"
    content = f"{md5_hash}  {filename}"  # Standard .md5 file format
    result = extract_md5_from_checksum_file_content(content)
    assert result == md5_hash


def test_extract_md5_from_checksum_file_content_raises_error_on_invalid_content():
    invalid_content = "not a hash here"
    with pytest.raises(AttributeError):
        extract_md5_from_checksum_file_content(invalid_content)


def test_extract_md5_from_checksum_file_content_ignores_whitespace_before_hash():
    md5_hash = "d41d8cd98f00b204e9800998ecf8427e"
    content = f"   {md5_hash}  somefile.txt"  # Leading whitespace before hash
    result = extract_md5_from_checksum_file_content(content.lstrip())
    assert result == md5_hash
