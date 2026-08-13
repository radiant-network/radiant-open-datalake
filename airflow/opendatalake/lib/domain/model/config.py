from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class DownloadConfig:
    """
    Encapsulates all parameters needed to download a file resource.

    Features:
        - Support for HTTP headers in the download request
        - Extraction of specific members from an archive (e.g., tar files)
        - Streaming S3 upload (bypassing local disk and extraction). Tip: Useful for large files.
        - Optional MD5 checksum handling

    Use `get_url(version)` to retrieve the download URL for a specific version.

    The `download_url` attribute can be either:
    - A string (for a fixed URL)
    - A callable that takes a version as a parameter and returns the URL (for dynamic URLs)
    - None, when `url_from_param=True` (the URL arrives at trigger time instead)

    If `extract_members` is provided (e.g., `extract_members=["file1.txt", "file2.txt"]`),
    the downloaded file is assumed to be a tar archive, and only the specified members will
    be extracted and copied.
    """

    download_url: str | Callable[[str], str] | None = None
    name: str | None = None
    headers: dict | Callable[[], dict] | None = None
    extract_members: list[str] | None = None
    use_stream_upload: bool = False
    use_stream_unzip: bool = False
    member_pattern: str | None = None
    url_from_param: bool = False
    md5_present: bool = False
    label: str | None = None  # Optional, use for display purposes in airflow UI
    secret_env_vars: tuple[tuple[str, str], ...] = ()

    def __post_init__(self):
        if not self.url_from_param and not self.download_url:
            raise ValueError("download_url must be provided as either a `str` or a `Callable`")

        if self.url_from_param and self.download_url:
            raise ValueError("url_from_param takes the URL at runtime; do not also set download_url")

        if self.use_stream_upload and self.extract_members:
            raise ValueError("stream upload does not support tar extract")

        if self.use_stream_unzip and (self.use_stream_upload or self.extract_members):
            raise ValueError("stream unzip is exclusive with stream upload and tar extract")

    def get_url(self, version: str) -> str:
        if not self.download_url:
            raise ValueError("download_url is not set on this config; the URL is supplied at runtime")
        return self.download_url if isinstance(self.download_url, str) else self.download_url(version)

    def get_headers(self) -> dict:
        if self.headers is None:
            return {}
        return self.headers() if callable(self.headers) else self.headers


class UpdateMode(Enum):
    MANUAL = "manual"
    AUTO = "auto"


@dataclass(frozen=True)
class ImportConfig:
    spark_command: str
    spark_conf: dict[str, str] | None = None
    waiter_max_attempts: int | None = None


@dataclass(frozen=True)
class SourceConfig:
    short_name: str
    display_name: str
    website: str
    download_configs: list[DownloadConfig]
    update_mode: UpdateMode = UpdateMode.MANUAL
    import_config: ImportConfig | None = None

    def get_latest_version(self) -> str:
        """Override this method to specify how to retrieve the latest version for a source.

        Returns:
            str: The latest detected version as a string
        """
        raise NotImplementedError()
