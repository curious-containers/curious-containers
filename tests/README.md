# Integration Tests

This readme provides an overview of the integration tests for the curious containers project. These integration tests are essential for ensuring that different components of the software work seamlessly together. The tests are performed using the pytest framework, and they involve setting up a test environment with Docker to mimic the deployment of a cc-agency.

## Prerequisites

Before running the integration tests, ensure the following prerequisites are met on the test system:

* Docker: Docker must be installed on the test system. You can download and install Docker from Docker's official website.
* User Permissions: The user executing the tests should have appropriate permissions to create and run Docker containers. This may require adding the user to the Docker group.

## Integration Test Files

The integration tests are organized into multiple test files, each targeting specific aspects of the software. These test files help ensure that various components work together as expected. However, note that the cc-faice component is currently not included in the integration tests.

## Automatic CC Agency Setup

The integration tests automatically set up a CC Agency for testing. The setup process involves creating Docker containers with the current source code and deploying the following components:

* agency_broker
* agency_controller
* agency_agency_trustee
* mongodb
* openssh-server (is deployed to verify the functionality of connectors)

## Running the Integration Tests

To execute the integration tests, follow these steps:

1. Ensure that Docker and Docker Compose are installed and the user has the necessary permissions to create and run Docker containers.
2. Navigate to the /tests directory of the software project.
3. Run the pytest command to initiate the integration tests. The pytest framework will automatically discover and execute the integration test files.
