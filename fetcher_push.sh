./mill service[2.12.18].assembly && \
./mill cloud_gcp[2.12.18].assembly && \
mkdir -p build_output && \
cp out/service/2.12.18/assembly.dest/out.jar build_output/service_assembly_deploy.jar && \
cp out/cloud_gcp/2.12.18/assembly.dest/out.jar build_output/cloud_gcp_lib_deploy.jar && \
docker login && \
docker buildx build \
  -f docker/fetcher/Dockerfile \
  --platform linux/amd64,linux/arm64 \
  -t ziplineai/chronon-fetcher:$(git rev-parse --short HEAD) \
  -t ziplineai/chronon-fetcher:latest \
  --push \
  .
