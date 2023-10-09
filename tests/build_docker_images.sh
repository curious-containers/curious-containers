#!/bin/sh

docker build -t "cc_broker:latest" "cc-agency/docker/image_agency_broker"
docker build -t "cc_controller:latest" "cc-agency/docker/image_agency_controller"
docker build -t "cc_trustee:latest" "cc-agency/docker/image_agency_trustee"
docker build -t "cc_mongodb:latest" "cc-agency/docker/image_mongodb"
