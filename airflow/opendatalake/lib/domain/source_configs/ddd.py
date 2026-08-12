import re
from dataclasses import dataclass
from datetime import datetime
from typing import override

from opendatalake.lib.domain.model.config import SourceConfig
from opendatalake.lib.utils.http import http_get


@dataclass(frozen=True, kw_only=True)
class DDDSourceConfig(SourceConfig):
    listing_url: str
    _VERSION_PATTERN = re.compile(r'href="(\d{4}_\d{2}_\d{2})/"')
    _VERSION_DATE_FORMAT = "%Y_%m_%d"

    @override
    def get_latest_version(self) -> str:
        html = http_get(self.listing_url).text
        versions = self._VERSION_PATTERN.findall(html)
        if not versions:
            raise ValueError(f"No G2P versions found at {self.listing_url}")
        latest = max(versions)
        try:
            datetime.strptime(latest, self._VERSION_DATE_FORMAT)
        except ValueError as e:
            raise ValueError(
                f"G2P version {latest!r} is not a valid date (expected {self._VERSION_DATE_FORMAT})"
            ) from e
        return latest
