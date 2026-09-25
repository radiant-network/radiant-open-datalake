import re
from dataclasses import dataclass
from typing import override

from opendatalake.lib.domain.model.config import SourceConfig
from opendatalake.lib.utils.http import http_get


class ClinvarSourceConfig(SourceConfig):
    @override
    def get_latest_version(self) -> str:
        md5_url = self.download_configs[0].download_url + ".md5"
        text = http_get(md5_url).text
        match = re.search(r"clinvar_([0-9]+)\.vcf", text)
        if not match:
            raise ValueError(f"Could not parse ClinVar version from {md5_url}")
        return match.group(1)


@dataclass(frozen=True, kw_only=True)
class ClinvarRcvSourceConfig(SourceConfig):
    """ClinVar RCV release source config.

    The RCV half of the monthly ClinVar XML release, a different product from the ClinVar VCF that
    `ClinvarSourceConfig` covers. The version is the release month, `YYYY-MM`.

    The release month cannot be read from the checksum the way the VCF's is: the `00-latest` symlink's
    `.md5` names `ClinVarRCVRelease_00-latest.xml.gz`, not the month behind it. So the versioned releases
    are listed instead, and the newest one that has its `.md5` companion published is taken -- NCBI
    uploads the two separately, so a release can be listed for minutes before it is complete.

    Ref: https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/_README
    """

    listing_url: str
    _RELEASE_PATTERN = re.compile(r"ClinVarRCVRelease_(\d{4}-\d{2})\.xml\.gz(\.md5)?")

    @override
    def get_latest_version(self) -> str:
        html = http_get(self.listing_url).text
        matches = set(self._RELEASE_PATTERN.findall(html))

        releases = {version for version, md5_suffix in matches if not md5_suffix}
        checksums = {version for version, md5_suffix in matches if md5_suffix}
        if not releases:
            raise ValueError(f"No ClinVar RCV releases found at {self.listing_url}")

        complete = releases & checksums
        if not complete:
            raise ValueError(f"No ClinVar RCV release has a .md5 companion at {self.listing_url}")
        return max(complete)
