import hashlib
import re


def compute_file_md5(path: str, chunk_size: int = 8192) -> str:
    """
    Compute the MD5 hash of a file.
    Args:
        path (str): Path to the file.
        chunk_size (int, optional): Number of bytes to read at a time. Defaults to 8192.

    Returns:
        str: The hexadecimal MD5 hash of the file's contents.
    """
    md5 = hashlib.md5()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            md5.update(chunk)
        return md5.hexdigest()


def check_md5(path: str, expected_md5) -> str:
    """
    Verify that a file's MD5 hash matches the expected value.

    Args:
        path (str): Path to the file.
        expected_md5 (str): The expected MD5 hash.

    Returns:
        str: The computed MD5 hash.

    Raises:
        AssertionError: If the computed hash does not match the expected value.
    """
    md5 = compute_file_md5(path)
    assert expected_md5 == md5, f"MD5 checksum verification failed for {path}, expected {expected_md5} but got {md5}"
    return md5


def extract_md5_from_checksum_file_content(md5_file_content: str) -> str:
    """
    Extract the MD5 hash from the content of a checksum (.md5) file.

    Args:
        md5_file_content (str): The content of the checksum file as a string.

    Returns:
        str: The extracted MD5 hash.

    Raises:
        AttributeError: If no valid MD5 hash is found in the content.
    """
    return re.search("^([0-9a-f]+)", md5_file_content).group(1)
