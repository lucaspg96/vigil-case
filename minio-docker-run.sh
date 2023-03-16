docker run \
   -p 9000:9000 \
   -p 9090:9090 \
   --name minio \
   --rm \
   -e "MINIO_ROOT_USER=user" \
   -e "MINIO_ROOT_PASSWORD=password" \
   quay.io/minio/minio server /data --console-address ":9090"