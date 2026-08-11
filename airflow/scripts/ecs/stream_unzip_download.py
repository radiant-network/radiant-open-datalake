import argparse
import logging
import sys

from opendatalake.lib.domain.download import S3Downloader
from opendatalake.lib.domain.model.sources import get_download_config_at_index

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


def main(source: str, prefix: str, version: str, download_index: int, download_url: str):
    if not download_url:
        raise ValueError("--download_url is required (the manually-provided archive URL)")

    download_conf = get_download_config_at_index(source, download_index)

    downloader = S3Downloader(s3_prefix=prefix, version=version, download_conf=download_conf)
    downloader.stream_unzip_upload(download_url)
    logger.info(f"Stream-unzip completed for source {source} version {version} to prefix {prefix}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream-unzip an archive URL and upload members to S3")
    parser.add_argument("--source", required=True, help="Source ID")
    parser.add_argument("--version", required=True, help="Version to upload")
    parser.add_argument("--prefix", required=True, help="S3 prefix to upload to")
    parser.add_argument(
        "--download_index", type=int, required=True, help="Index of the download config to use for the source"
    )
    parser.add_argument("--download_url", required=True, help="Direct URL of the .zip archive to stream")

    args = parser.parse_args()
    # Note: the URL is intentionally not logged -- it may be a signed/tokened link.
    logger.info(f"Stream-unzip args: source={args.source} version={args.version} prefix={args.prefix}")

    try:
        main(
            source=args.source,
            prefix=args.prefix,
            version=args.version,
            download_index=args.download_index,
            download_url=args.download_url,
        )
    except Exception as e:
        logger.exception(f"Error while processing task: {e}")
        sys.exit(1)
