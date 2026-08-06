import logging
import re
from dataclasses import dataclass
from typing import override

from opendatalake.lib.domain.model.config import SourceConfig
from opendatalake.lib.utils.http import http_get

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class OneThousandGenomesSourceConfig(SourceConfig):
    """
    1000 Genomes Project source config.
    """

    listing_url: str
    _RELEASE_DIR_PATTERN = re.compile(r'href="(\d{4}_\d{2}|\d{8})/"')

    @override
    def get_latest_version(self) -> str:
        html = http_get(self.listing_url).text
        releases = self._RELEASE_DIR_PATTERN.findall(html)
        if not releases:
            raise ValueError(f"No 1000 Genomes release directory found at {self.listing_url}")

        latest = max(releases, key=self._release_sort_key)
        LOGGER.info(f"Found latest 1000 Genomes release: {latest}")
        return latest

    @staticmethod
    def _release_sort_key(release: str) -> int:
        return int(release.replace("_", "").ljust(8, "0"))
