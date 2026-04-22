import logging
import re
from dataclasses import dataclass
from typing import override

from dags.lib.domain.model.config import SourceConfig
from dags.lib.utils.http import http_get
from dags.lib.utils.md5 import extract_md5_from_checksum_file_content

LOGGER = logging.getLogger(__name__)

REFSEQ_ACCESSION_NUMBER = "GCF_000001405"


@dataclass(frozen=True, kw_only=True)
class DBSNPSourceConfig(SourceConfig):
    """dbSNP source config.

    Return only the latest accession if valid.
    (with both `.gz` AND `.gz.md5` checksum present to be considered valid)

    Ref: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/data-processing/policies-annotation/genome-processing/version-status/
    """
    listing_url: str
    _REFSEQ_FILE_PATTERN = re.compile(rf"{REFSEQ_ACCESSION_NUMBER}\.\d+\.gz(?:\.md5)?")
    _REFSEQ_ACCESSION_PATTERN = re.compile(rf"^({REFSEQ_ACCESSION_NUMBER})\.(\d+)$")

    @override
    def get_latest_version(self) -> str:
        return self._get_latest_ref_seq(listing_url=self.listing_url)

    @classmethod
    def _parse_ref_seq(cls, filename: str) -> dict:
        match = cls._REFSEQ_ACCESSION_PATTERN.match(filename)
        if not match:
            raise ValueError(f"Invalid RefSeq filename: {filename}")

        accession, version = match.groups()
        return {
            "accession": accession,
            "version": int(version),
            "full": filename,
        }

    @classmethod
    def _get_latest_ref_seq(cls, listing_url: str) -> str:
        html = http_get(listing_url).text
        files = set(cls._REFSEQ_FILE_PATTERN.findall(html))

        accessions = [cls._parse_ref_seq(f.removesuffix(".gz")) for f in files if f.endswith(".gz")]
        if not accessions:
            raise ValueError(f"No RefSeq accessions found at: {listing_url}")

        latest = max(accessions, key=lambda x: x["version"])
        LOGGER.info(f"Found latest RefSeq accession: {str(latest)}")

        if f"{latest['full']}.gz.md5" not in files:
            raise ValueError(f"Latest RefSeq {latest['full']} is missing .md5 companion at: {listing_url}")

        cls._verify_md5_digest(listing_url=listing_url, version=latest["version"])
        return f"{REFSEQ_ACCESSION_NUMBER}.{latest["version"]}"

    @staticmethod
    def _verify_md5_digest(listing_url: str, version: int) -> None:
        md5_url = f"{listing_url.rstrip('/')}/{REFSEQ_ACCESSION_NUMBER}.{version}.gz.md5"
        md5_body = http_get(md5_url).text
        md5_raw = md5_body.split(" ")[0]
        try:
            extract_md5_from_checksum_file_content(md5_raw)
        except AttributeError as ae:
            raise ValueError(f"Invalid MD5 digest retrieved from {md5_body}") from ae
