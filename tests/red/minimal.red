cli:
  baseCommand: echo
  class: CommandLineTool
  cwlVersion: v1.0
  inputs:
    some_string:
      inputBinding:
        position: 0
      type: string
  outputs: {}
container:
  engine: docker
  settings:
    image:
      url: python:3.12
    ram: 256
execution:
  engine: ccagency
  settings:
    access:
      auth:
        password: '{{agency_password}}'
        username: '{{agency_user}}'
      url: '{{agency_url}}'
inputs:
  some_string: test
outputs: {}
redVersion: '9'
