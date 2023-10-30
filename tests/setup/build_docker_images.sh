#!/bin/sh

cd ..

docker build -t "cc_broker:latest" -f "tests/setup/image_agency_broker/Dockerfile" .
docker build -t "cc_controller:latest" -f "tests/setup/image_agency_controller/Dockerfile" .
docker build -t "cc_trustee:latest" -f "tests/setup/image_agency_trustee/Dockerfile" .
docker build -t "cc_mongodb:latest" -f "tests/setup/image_mongodb/Dockerfile" .
