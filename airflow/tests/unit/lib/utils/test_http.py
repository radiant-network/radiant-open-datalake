import socket
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests_mock

from opendatalake.lib.utils.http import _MSSClampAdapter, get_session, http_get, stream_download_file


@pytest.mark.skipif(not hasattr(socket, "TCP_MAXSEG"), reason="TCP_MAXSEG not available on this platform")
def test_mss_clamp_adapter_sets_tcp_maxseg():
    adapter = _MSSClampAdapter(mss=1200)
    socket_options = adapter.poolmanager.connection_pool_kw["socket_options"]
    assert (socket.IPPROTO_TCP, socket.TCP_MAXSEG, 1200) in socket_options
    # Must not clobber urllib3's defaults (e.g. TCP_NODELAY).
    from urllib3.connection import HTTPConnection

    for default_option in HTTPConnection.default_socket_options:
        assert default_option in socket_options


def test_get_session_returns_shared_singleton():
    assert get_session() is get_session()
    assert isinstance(get_session(), requests.Session)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="MSS clamp adapter is mounted on Linux only")
def test_session_mounts_mss_adapter_on_linux():
    adapter = get_session().get_adapter("https://ftp.ncbi.nlm.nih.gov/")
    assert isinstance(adapter, _MSSClampAdapter)


def test_http_get_successful_response():
    url = "http://example.com"
    with requests_mock.Mocker() as m:
        m.get(url, text="hello", status_code=200)
        response = http_get(url)
        assert response.status_code == 200
        assert response.text == "hello"


def test_http_get_passes_headers():
    url = "http://example.com"
    headers = {"Authorization": "Bearer token"}
    with requests_mock.Mocker() as m:
        m.get(url, text="hello", status_code=200)
        response = http_get(url, headers=headers)

        assert response.status_code == 200
        assert response.text == "hello"

        # Check that the request included the correct header
        assert m.last_request.headers.get("Authorization") == "Bearer token"


def test_http_get_raises_for_unsuccessful_status_code():
    url = "http://example.com"
    with requests_mock.Mocker() as m:
        m.get(url, status_code=404)
        with pytest.raises(requests.HTTPError):
            http_get(url)


def test_http_get_passes_timeout():
    url = "http://example.com"
    with requests_mock.Mocker() as m:
        m.get(url, text="hello", status_code=200)
        http_get(url)
        assert m.last_request.timeout == (10, 60)


def test_stream_download_file_happy_path(tmp_path):
    url = "http://example.com/file.txt"
    content = b"hello world"
    dest_file = tmp_path / "downloaded.txt"

    with requests_mock.Mocker() as m:
        m.get(url, content=content, status_code=200)
        stream_download_file(url, str(dest_file))

        assert m.last_request.stream

    # Check file exists and content matches
    assert dest_file.exists()
    with open(dest_file, "rb") as f:
        assert f.read() == content


def test_stream_download_file_passes_headers(tmp_path):
    url = "http://example.com/file.txt"
    content = b"header test"
    dest_file = tmp_path / "downloaded.txt"
    headers = {"Authorization": "Bearer token"}

    with requests_mock.Mocker() as m:
        m.get(url, content=content, status_code=200)
        stream_download_file(url, str(dest_file), headers=headers)

        # Check that the request included the correct header
        assert m.last_request.headers.get("Authorization") == "Bearer token"

    # Check file exists and content matches
    assert dest_file.exists()
    with open(dest_file, "rb") as f:
        assert f.read() == content


def test_stream_download_file_respects_chunk_size(tmp_path):
    url = "http://example.com/file.txt"
    content = b"abcdefghij"  # 10 bytes
    dest_file = tmp_path / "downloaded.txt"
    expected_chunk_size = 3

    mock_response = MagicMock()
    mock_response.__enter__.return_value = mock_response
    mock_response.raise_for_status.return_value = None

    def fake_iter_content(chunk_size):
        for i in range(0, len(content), chunk_size):
            yield content[i : i + chunk_size]

    mock_response.iter_content.side_effect = fake_iter_content

    # Downloads go through the shared MSS-clamped session, not the module-level requests.get.
    with patch("opendatalake.lib.utils.http._SESSION.get", return_value=mock_response):
        stream_download_file(url, str(dest_file), chunk_size=expected_chunk_size)

    # Check file exists and content matches
    assert dest_file.exists()
    with open(dest_file, "rb") as f:
        assert f.read() == content

    # Check that the right chunk size was passed
    mock_response.iter_content.assert_called_once_with(chunk_size=expected_chunk_size)


def test_stream_download_file_raises_for_unsuccessful_status_code(tmp_path):
    url = "http://example.com"
    dest_file = tmp_path / "downloaded.txt"

    with requests_mock.Mocker() as m:
        m.get(url, status_code=404)
        with pytest.raises(requests.HTTPError):
            stream_download_file(url, str(dest_file))


def test_stream_download_file_overwrites_existing_file(tmp_path):
    url = "http://example.com/file.txt"
    original_content = b"original"
    new_content = b"new content"
    dest_file = tmp_path / "downloaded.txt"

    # Write original content to the file
    with open(dest_file, "wb") as f:
        f.write(original_content)

    # Download new content to the same file
    with requests_mock.Mocker() as m:
        m.get(url, content=new_content, status_code=200)
        stream_download_file(url, str(dest_file))

    # Check that the file now contains the new content (was overwritten)
    assert dest_file.exists()
    with open(dest_file, "rb") as f:
        assert f.read() == new_content
