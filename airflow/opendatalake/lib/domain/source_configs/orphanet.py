import re
from typing import override

from opendatalake.lib.domain.model.config import SourceConfig
from opendatalake.lib.utils.http import http_get


class OrphanetSourceConfig(SourceConfig):
    _VERSION_PATTERN = re.compile(r'<JDBOR\b[^>]*\bversion="([^"]+)"')
    _HEADER_BYTES = 8192

    @override
    def get_latest_version(self) -> str:
        genes_url = self.download_configs[0].get_url("")
        text = http_get(genes_url, headers={"Range": f"bytes=0-{self._HEADER_BYTES - 1}"}).text
        match = self._VERSION_PATTERN.search(text)
        if not match:
            raise ValueError(f"Could not parse the Orphanet <JDBOR> version attribute at {genes_url}")
        return self._normalize_version(match.group(1))

    @staticmethod
    def _normalize_version(raw: str) -> str:
        head = raw.split("(", 1)[0]
        return re.sub(r"[^0-9A-Za-z._-]+", "_", head).strip("_")
