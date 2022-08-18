# Docker Images

Production Docker images are automatically built and published to [Metaphor's Docker Hub](https://hub.docker.com/r/metaphordata/connectors) as part of the CI/CD pipeline.

To build and publish an image with the `test` tag for testing, run the following commands,

```sh
# Assuming not yet logged in
docker login

docker build --platform linux/amd64 -t metaphordata/connectors:test .
docker push metaphordata/connectors:test
```
