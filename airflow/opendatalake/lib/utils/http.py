"""
HTTP utility functions for downloading files and making GET requests.

Downloads go through the shared session returned by ``get_session``, whose sockets
advertise a small TCP Maximum Segment Size (MSS clamping) so remote servers never send
oversized segments. On egress paths where Path-MTU Discovery is broken — e.g. an ECS
Fargate ENI (jumbo 9001 MTU) reaching the internet through a 1500-MTU NAT with ICMP
"fragmentation needed" blocked — full-size TLS-handshake packets are silently dropped
(a PMTU black hole), so the TLS handshake hangs. Clamping the MSS keeps every packet
small enough to survive that path. Clamp size: OPENDATALAKE_HTTP_MSS (default 1200, safe
under IPv6 and common tunneling overhead). Applied on Linux only (production); a no-op
elsewhere, where TCP_MAXSEG may be unsettable pre-connect.
"""

import logging
import os
import socket
import sys
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection

logger = logging.getLogger(__name__)

# (connect timeout, read timeout) in seconds. Read timeout is per-read, not total,
# so it is safe for large streamed downloads.
_DEFAULT_TIMEOUT = (10, 60)

# Advertised TCP MSS. 1200 stays under the 1280-byte IPv6 minimum MTU even after
# tunneling overhead, so handshake packets survive a PMTU-black-holed egress path.
_HTTP_MSS = int(os.getenv("OPENDATALAKE_HTTP_MSS", "1200"))


class _MSSClampAdapter(HTTPAdapter):
    """
    Requests adapter that pins the TCP Maximum Segment Size on every connection.

    A small advertised MSS forces the peer to send small segments, so oversized
    packets never exist to be dropped on a PMTU-black-holed path. TCP_NODELAY (the
    urllib3 default) is preserved.
    """

    def __init__(self, mss: int, **kwargs):
        self._mss = mss
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["socket_options"] = HTTPConnection.default_socket_options + [
            (socket.IPPROTO_TCP, socket.TCP_MAXSEG, self._mss),
        ]
        super().init_poolmanager(*args, **kwargs)


def _build_session() -> requests.Session:
    """Session with the MSS-clamp adapter mounted (Linux only; no-op elsewhere)."""
    session = requests.Session()
    if sys.platform.startswith("linux") and hasattr(socket, "TCP_MAXSEG"):
        adapter = _MSSClampAdapter(mss=_HTTP_MSS)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        logger.debug("HTTP session mounted with MSS clamp = %d", _HTTP_MSS)
    return session


# Module-level shared session so connection pooling and the MSS clamp apply everywhere.
_SESSION = _build_session()


def get_session() -> requests.Session:
    """Shared HTTP session with the MSS clamp applied. Reuse it for all outbound downloads."""
    return _SESSION


def stream_download_file(
    url: str,
    dest_file_name: str,
    headers: Any = None,
    chunk_size: int = 8192,
    timeout: Any = _DEFAULT_TIMEOUT,
    **kwargs,
) -> None:
    """
    Downloads a file from the specified URL and saves it to the given destination path.
    The download is streamed in chunks to handle large files efficiently.

    Args:
        url (str): The URL to download the file from.
        dest_file_name (str): The local file path where the downloaded content will be saved.
        headers (Any, optional): Optional HTTP headers to include in the request.
        chunk_size (int, optional): The size (in bytes) of each chunk to read from the response. Defaults to 8192.
        timeout (Any, optional): requests timeout as (connect, read) seconds. Defaults to (10, 60).
        **kwargs: Additional keyword arguments passed to `requests.get`.

    Raises:
        requests.HTTPError: If the HTTP request returned an unsuccessful status code.

    Notes:
        Empty chunks (used for keep-alive) are ignored.
    """
    with _SESSION.get(url, headers=headers, stream=True, timeout=timeout, **kwargs) as response:
        response.raise_for_status()
        with open(dest_file_name, "wb") as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out empty chunks sent to keep the connection alive in streaming mode
                    file.write(chunk)


def http_get(url: str, headers: Any = None, timeout: Any = _DEFAULT_TIMEOUT) -> requests.Response:
    """
    Sends a GET request to the specified URL and returns the response object.

    Args:
        url (str): The URL to send the GET request to.
        headers (Any, optional): Optional HTTP headers to include in the request.
        timeout (Any, optional): requests timeout as (connect, read) seconds. Defaults to (10, 60).

    Returns:
        requests.Response: The response object from the HTTP request.

    Raises:
        requests.HTTPError: If the HTTP request returned an unsuccessful status code.
    """
    response = _SESSION.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response
