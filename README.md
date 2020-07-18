# LCLS AutoSFX workflows
development and maintenance of (Airflows) workflows for automated SFX processing at LCLS

## Docker image
Available on DockerHub: [slaclcls/crystfel:latest](https://hub.docker.com/repository/registry-1.docker.io/slaclcls/crystfel/tags?page=1)

- added CCP4 and XDS to [Dockerfile.crystfel](https://github.com/fredericpoitevin/relmanage/blob/crystfel-docker-image-for-cori/docker/nersc/docker/Dockerfile.crystfel)
- on personal MacBook:
```bash
docker build -t crystfel -f docker/Dockerfile.crystfel .
docker push slaclcls/crystfel:latest
```
- on Cori: 
```bash
shifterimg -v pull slaclcls/crystfel:latest
```

