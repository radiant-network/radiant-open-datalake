import re
from dataclasses import dataclass
from datetime import datetime
from typing import override

from opendatalake.lib.domain.model.config import SourceConfig
from opendatalake.lib.utils.http import http_get


@dataclass(frozen=True, kw_only=True)
class HpoSourceConfig(SourceConfig):
    """Human Phenotype Ontology (HPO) source config.

    The latest release: https://github.com/obophenotype/human-phenotype-ontology/releases/latest

    Will be redirected by Github to the specific date-based tag (e.g. ``v2026-06-23``).
    """

    latest_release_url: str
    _TAG_PATTERN = re.compile(r"/releases/tag/([\w.\-]+)")
    _VERSION_DATE_FORMAT = "v%Y-%m-%d"

    @override
    def get_latest_version(self) -> str:
        resolved_url = http_get(self.latest_release_url).url
        match = self._TAG_PATTERN.search(resolved_url)
        if not match:
            raise ValueError(f"Could not parse HPO version from {resolved_url}")
        tag = match.group(1)
        try:
            datetime.strptime(tag, self._VERSION_DATE_FORMAT)
        except ValueError as e:
            raise ValueError(f"HPO version {tag!r} is not a valid date (expected {self._VERSION_DATE_FORMAT})") from e
        return tag
