"""
HTTP utility functions for downloading files and making GET requests.

Note:
    These functions do not implement automatic retry logic for failed requests.
    Consider adding retries at the DAG/task level, or directly in these utilities
    if robustness is needed.
"""

from typing import Any

import requests


def stream_download_file(url: str, dest_file_name: str, headers: Any = None, chunk_size: int = 8192, **kwargs) -> None:
    """
    Downloads a file from the specified URL and saves it to the given destination path.
    The download is streamed in chunks to handle large files efficiently.

    Args:
        url (str): The URL to download the file from.
        dest_file_name (str): The local file path where the downloaded content will be saved.
        headers (Any, optional): Optional HTTP headers to include in the request.
        chunk_size (int, optional): The size (in bytes) of each chunk to read from the response. Defaults to 8192.
        **kwargs: Additional keyword arguments passed to `requests.get`.

    Raises:
        requests.HTTPError: If the HTTP request returned an unsuccessful status code.

    Notes:
        Empty chunks (used for keep-alive) are ignored.
    """
    with requests.get(url, headers=headers, stream=True, **kwargs) as response:
        response.raise_for_status()
        with open(dest_file_name, "wb") as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out empty chunks sent to keep the connection alive in streaming mode
                    file.write(chunk)


def http_get(url: str, headers: Any = None) -> requests.Response:
    """
    Sends a GET request to the specified URL and returns the response object.

    Args:
        url (str): The URL to send the GET request to.
        headers (Any, optional): Optional HTTP headers to include in the request.

    Returns:
        requests.Response: The response object from the HTTP request.

    Raises:
        requests.HTTPError: If the HTTP request returned an unsuccessful status code.
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response
