redVersion: "9"
cli:
  cwlVersion: "v1.0"
  class: "CommandLineTool"
  baseCommand: "count_item.py"
  inputs:
    input_file:
      type: "File"
      inputBinding:
        position: 0
    item:
      type: "string"
      inputBinding:
        prefix: "--item"
        separate: true
  outputs:
    output_file:
      type: "File"
      outputBinding:
        glob: "output.txt"
batches:
  - inputs:
      item:
        "C++"
      input_file:
        class: "File"
        connector:
          command: "red-connector-http"
          access:
            url: "https://github.com/"
    outputs:
      output_file:
        class: "File"
        connector:
          command: "red-connector-http"
          access:
            url: "https://github.com/"
container:
  engine: "docker"
  settings:
    image:
      url: "python:latest"
    ram: 256
execution:
  engine: "ccfaice"
  settings: {}