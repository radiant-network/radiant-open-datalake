from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class DownloadConfig:
    """
    This class encapsulates all parameters needed to download a file resource,
    including support for HTTP headers, archive extraction, direct S3 upload,
    and optional MD5 checksum handling.

    Use `get_url()` to retrieve the download URL
    """

    name: str | None = None
    headers: dict | None = None
    extract_members: list[str] | None = (None,)
    use_direct_upload: bool = (False,)
    md5_present: bool = False

    def __init__(
        self,
        name: str | None = None,
        url: str | None = None,
        url_fn: Callable[[], str] | None = None,
        headers: dict | None = None,
        extract_members: list[str] | None = None,
        use_direct_upload: bool = False,
        md5_present: bool = False,
    ):
        """
        Args:
            name (Optional[str]): Destination file name. If not set, the name is inferred from the URL.
            url (Optional[str]): Direct URL to download the file.
            url_fn (Optional[callable]): Function returning the URL to download the file.
            headers (Optional[dict]): Optional HTTP headers for the download request.
            extract_members (Optional[list[str]]): List of member names to extract if the file is an archive
                (e.g., tar).
            use_direct_upload (bool): If True, stream file directly to S3 (no local copy,
                no tar extraction, no MD5 check).
            md5_present (bool): If True, an associated MD5 checksum file is available (same URL + ".md5").
        Raises:
            AssertionError: If both or neither of `url` and `url_fn` are provided, or if direct upload is requested
            with tar extraction.
        """
        assert url or url_fn, "Either url or url_fn must be provided"
        assert not (url and url_fn), "Specify only one of url or url_fn"
        assert not (use_direct_upload and extract_members), "direct upload does not support tar extract"

        object.__setattr__(self, "_url", url)
        object.__setattr__(self, "_url_fn", url_fn)
        object.__setattr__(self, "headers", headers)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "extract_members", extract_members)
        object.__setattr__(self, "use_direct_upload", use_direct_upload)
        object.__setattr__(self, "md5_present", md5_present)

    def get_url(self) -> str:
        return self._url if self._url else self._url_fn()
