from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class DownloadConfig:
    """
    Encapsulates all parameters needed to download a file resource.

    Features:
        - Support for HTTP headers in the download request
        - Extraction of specific members from an archive (e.g., tar files)
        - Direct S3 upload (bypassing local copy and extraction)
        - Optional MD5 checksum handling

    Use `get_url(version)` to retrieve the download URL for a specific version.

    The `download_url` attribute can be either:
    - A string (for a fixed URL)
    - A callable that takes a version as a parameter and returns the URL (for dynamic URLs)

    The download_url attribute should be a string (fixed url) or a callable
    taking a version in parameter and returning the url (dynamic url).

    If `extract_members` is provided (e.g., `extract_members=["file1.txt", "file2.txt"]`),
    the downloaded file is assumed to be a tar archive, and only the specified members will
    be extracted and copied.
    """

    download_url: str | Callable[[str], str]
    name: str | None = None
    headers: dict | None = None
    extract_members: list[str] | None = None
    use_direct_upload: bool = False
    md5_present: bool = False

    def __post_init__(self):
        if not self.download_url:
            raise ValueError("download_url must be provided as either a `str` or a `Callable`")

        if self.use_direct_upload and self.extract_members:
            raise ValueError("direct upload does not support tar extract")

    def get_url(self, version: str) -> str:
        return self.download_url if isinstance(self.download_url, str) else self.download_url(version)
