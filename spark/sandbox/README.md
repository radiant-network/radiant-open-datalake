# Local Sandbox

## 1. Prerequisites

You need to install the following:
- Docker
- Java 11
- Sbt
- Spark 3.5


## 2. Starts minio and rest iceberg catalog

```sh
  docker-compose up -d
```


## 3. Upload raw data to minio

Here is an example for clinvar using the `mc` client, assuming you downloaded a local copy of the raw clinvar.vcf.gz file:
```sh
  mc alias set localminio http://127.0.0.1:9000 admin password
  mc cp clinvar.vcf.gz localminio/opendatalake-qa/raw/landing/clinvar/    
```

Alternatively, you can use the ui. Login to localhost:9001 with admin, password as username and password.


## 4. Build the assembly

```sh
  sbt "runMain org.radiant.opendatalake.config.EtlConfiguration"
  sbt clean assembly
```

## 5. Run spark-submit command

Here is an example spark-submit command running the clinvar normalization job:
```sh
  spark-submit \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.access.key=admin  \
  --conf spark.hadoop.fs.s3a.secret.key=password  \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf  spark.hadoop.fs.s3a.endpoint.region=us-east-1  \
  --conf spark.sql.catalog.opendatalake.uri=http://localhost:8181/  \
  --conf spark.sql.catalog.opendatalake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.opendatalake.client.region=us-east-1 \
  --conf spark.sql.catalog.opendatalake.s3.access-key-id=admin  \
  --conf spark.sql.catalog.opendatalake.s3.secret-access-key=password  \
  --conf spark.sql.catalog.opendatalake.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.opendatalake.type=rest \
  --conf spark.sql.catalog.opendatalake.s3.path-style-access=true \
  --master local \
  --driver-memory 12g \
  --class org.radiant.opendatalake.ImportPublicTable  \
  target/scala-2.12/radiant-open-datalake-spark.jar \
  clinvar  '--config' config/qa.conf '--steps'  default  '--app-name' test
```

Note: We use path-style S3 access here for local testing with MinIO, as it is simpler to configure in a sandbox environment. However, path-style access is deprecated by AWS and may be removed in the future. 
For production deployments on AWS S3, it is recommended to use virtual-host-style access.