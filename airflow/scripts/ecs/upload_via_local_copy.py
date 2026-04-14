import argparse
import logging
import sys

from dags.lib.domain.download import upload_via_local_copy
from dags.lib.domain.model.sources import get_download_config

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(source: str, prefix: str, version: str, download_index: int):
    download_conf = get_download_config(source, download_index)

    upload_via_local_copy(s3_prefix=prefix, version=version, download_conf=download_conf)
    logger.info(f"Upload via local copy completed for source {source} version {version} to prefix {prefix}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload source via local copy")
    parser.add_argument("--source", required=True, help="Source ID")
    parser.add_argument("--version", required=True, help="Version to upload")
    parser.add_argument("--prefix", required=True, help="S3 prefix to upload to")
    parser.add_argument(
        "--download_index", type=int, required=True, help="Index of the download config to use for the source"
    )

    args = parser.parse_args()
    logger.info(f"Command line arguments: {args}")

    try:
        main(source=args.source, prefix=args.prefix, version=args.version, download_index=args.download_index)
    except Exception as e:
        logger.exception(f"Error while processing task: {e}")
        sys.exit(1)
