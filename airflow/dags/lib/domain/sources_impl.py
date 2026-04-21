import logging
import re
from typing import override

from dags.lib.domain.model.config import SourceConfig
from dags.lib.utils.http import http_get
from dags.lib.utils.md5 import extract_md5_from_checksum_file_content

LOGGER = logging.getLogger(__name__)


class ClinvarSourceConfig(SourceConfig):
    @override
    def get_latest_version(self) -> str:
        md5_url = self.download_configs[0].download_url + ".md5"
        text = http_get(md5_url).text
        match = re.search(r"clinvar_([0-9]+)\.vcf", text)
        if not match:
            raise ValueError(f"Could not parse ClinVar version from {md5_url}")
        return match.group(1)


class DBSNPSourceConfig(SourceConfig):
    """dbSNP source config.

    Return only the latest accession if valid.
    (with both `.gz` AND `.gz.md5` checksum present to be considered valid)

    Ref: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/data-processing/policies-annotation/genome-processing/version-status/
    """

    _REFSEQ_FILE_PATTERN = re.compile(r"GCF_\d{9}\.\d+\.gz(?:\.md5)?")
    _REFSEQ_ACCESSION_PATTERN = re.compile(r"^(GCF)_(\d{9})\.(\d+)$")

    @override
    def get_latest_version(self) -> str:
        url = self.download_configs[0].download_url(version="").removesuffix(".gz")
        return self._get_latest_ref_seq(url)

    @classmethod
    def _parse_ref_seq(cls, filename: str) -> dict:
        match = cls._REFSEQ_ACCESSION_PATTERN.match(filename)
        if not match:
            raise ValueError(f"Invalid RefSeq filename: {filename}")

        prefix, digits, version = match.groups()
        return {
            "prefix": prefix,
            "digits": digits,
            "version": int(version),
            "full": filename,
        }

    @classmethod
    def _get_latest_ref_seq(cls, url: str) -> str:
        html = http_get(url).text
        files = set(cls._REFSEQ_FILE_PATTERN.findall(html))

        accessions = [cls._parse_ref_seq(f.removesuffix(".gz")) for f in files if f.endswith(".gz")]
        if not accessions:
            raise ValueError(f"No RefSeq accessions found at: {url}")

        latest = max(accessions, key=lambda x: x["version"])
        LOGGER.info(f"Found latest RefSeq accession: {str(latest)}")

        if f"{latest['full']}.gz.md5" not in files:
            raise ValueError(f"Latest RefSeq {latest['full']} is missing .md5 companion at: {url}")

        cls._verify_md5_digest(url, latest["full"])
        return latest["full"]

    @classmethod
    def _verify_md5_digest(cls, listing_url: str, accession: str) -> None:
        md5_url = f"{listing_url.rstrip('/')}/{accession}.gz.md5"
        md5_body = http_get(md5_url).text
        md5_raw = md5_body.split(" ")[0]
        try:
            extract_md5_from_checksum_file_content(md5_raw)
        except AttributeError as ae:
            raise ValueError(f"Invalid MD5 digest retrieved from {md5_body}") from ae
