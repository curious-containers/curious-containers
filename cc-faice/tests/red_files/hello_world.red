redVersion: "9"
cli:
  cwlVersion: "v1.0"
  class: "CommandLineTool"
  baseCommand: "echo"
  inputs:
    some_string:
      type: "string"
      inputBinding:
        position: 0
  outputs:
    mystdout:
      type: "stdout"
  stdout: "stdout.txt"
inputs:
  some_string: "hello world"
outputs: {}
container:
  engine: "docker"
  settings:
    image:
      url: "python:latest"
    ram: 256
execution:
  engine: "ccfaice"
  settings: {}
