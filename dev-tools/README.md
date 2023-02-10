# Dev Tools

Tools to make it more easy to develope curious containers.


## Local CC-Agency Testing

### Usage
To build local development docker images for cc-agency execute `./build_dev_images.sh`.
This will build two new docker images `cc-agency-controller-tmp:0.0` and `cc-agency-broker-tmp:0.0` which can be used to test the agency locally.

To start an agency using these images run `docker compose --project-name=agency_in_docker_dev up` or `./run.sh`.

### Restrictions
The trustee-service is not updated!
