# Docker Images

Production Docker image is automatically built and published to [Metaphor's Docker Hub](https://hub.docker.com/r/metaphordata/connectors) as part of the CI/CD pipeline.

## Versioning & Tagging

We follow [Semantic Versioning](https://semver.org/) and [docker tagging best practices](https://stevelasker.blog/2018/03/01/docker-tagging-best-practices-for-tagging-and-versioning-docker-images/) closely so you can pin to a specific version (e.g. `metaphordata/connectors:0.12`) without worrying about break changes.

## Running

The Docker image contains all the required dependencies so running it should be very straightforward. Note that you'll need to provide your local config file to the Docker container via [bind mounts](https://docs.docker.com/storage/bind-mounts/). For example, this command passes your local config file (`~/config.yml`) to the docker image,

```sh
docker run -it --rm \
  -v ~/config.yml:/config.yml \
  metaphordata/connectors \
  metaphor bigquery /config.yml
```

If you configure to write the [output](../metaphor/common/docs/output.md) to a local directory instead of an S3 path, you'll also need to bind mount the output directory (`~/output` in this case) to access the files, e.g.

```yml
# ~/config.yml
# connector-specific config
# ...

output: 
  file:
    directory: /output
```

```sh
docker run -it --rm \
  -v ~/config.yml:/config.yml \
  -v ~/output:/output \
  metaphordata/connectors \
  metaphor bigquery /config.yml
```

## Building & Publishing

To build and publish an image with the `test` tag for testing, run the following commands,

```sh
# Assuming not yet logged in
docker login

docker build --platform linux/amd64 -t metaphordata/connectors:test .
docker push metaphordata/connectors:test
```
