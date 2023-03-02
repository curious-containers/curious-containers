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