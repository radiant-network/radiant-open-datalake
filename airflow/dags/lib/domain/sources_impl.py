import re
from typing import override

from dags.lib.domain.model.config import SourceConfig
from dags.lib.utils.http import http_get


class ClinvarSourceConfig(SourceConfig):
    @override
    def get_latest_version(self) -> str | None:
        md5_url = self.download_configs[0].download_url + ".md5"
        text = http_get(md5_url).text
        return re.search(r"clinvar_([0-9]+)\.vcf", text).group(1)


class DNSNPSourceConfig(SourceConfig):
    @override
    def get_latest_version(self) -> str | None:
        return None  # TODO Implement this
