docker run -d -p 9000:9000 -v $PWD/tests/s3/data:/data --name minio minio/minio:RELEASE.2021-11-03T03-36-36Z server /data
use this version for mounting files into minio