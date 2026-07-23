-- StarRocks external catalog over the Polaris-managed Iceberg tables (data in MinIO).
-- Polaris serves metadata via its REST API (OAuth2); StarRocks reads data files straight from MinIO
-- with static credentials (MinIO has no STS, so credential vending is disabled).
CREATE EXTERNAL CATALOG IF NOT EXISTS opendatalake
PROPERTIES (
    "type"                                       = "iceberg",
    "iceberg.catalog.type"                       = "rest",
    "iceberg.catalog.uri"                        = "http://polaris:8181/api/catalog",
    "iceberg.catalog.warehouse"                  = "opendatalake",
    "iceberg.catalog.security"                   = "oauth2",
    "iceberg.catalog.oauth2.credential"          = "root:s3cr3t",
    "iceberg.catalog.oauth2.scope"               = "PRINCIPAL_ROLE:ALL",
    "iceberg.catalog.vended-credentials-enabled" = "false",
    "aws.s3.endpoint"                            = "http://opendatalake-minio:9000",
    "aws.s3.enable_path_style_access"            = "true",
    "aws.s3.enable_ssl"                          = "false",
    "aws.s3.access_key"                          = "admin",
    "aws.s3.secret_key"                          = "password",
    "aws.s3.region"                              = "us-east-1"
);
