import re
from dataclasses import dataclass
from typing import override

from opendatalake.lib.domain.model.config import SourceConfig
from opendatalake.lib.utils.http import http_get


@dataclass(frozen=True, kw_only=True)
class MondoSourceConfig(SourceConfig):
    """Mondo Disease Ontology source config.

    The latest release: https://github.com/monarch-initiative/mondo/releases/latest

    Will be redirected by Github to the specific date-based tag (e.g. ``v2024-09-03``).
    """

    latest_release_url: str
    _TAG_PATTERN = re.compile(r"/releases/tag/([\w.\-]+)")

    @override
    def get_latest_version(self) -> str:
        resolved_url = http_get(self.latest_release_url).url
        match = self._TAG_PATTERN.search(resolved_url)
        if not match:
            raise ValueError(f"Could not parse Mondo version from {resolved_url}")
        return match.group(1)
