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
inputs:
  item:
    "C++"
  input_file:
    class: "File"
    connector:
      command: "red-connector-http"
      access:
        url: "https://www.stroustrup.com/C++.html"
outputs:
  output_file:
    class: "File"
    connector:
      command: "red-connector-http"
      access:
        url: "https://encyb3haf7gch.x.pipedream.net"
container:
  engine: "docker"
  settings:
    image:
      url: "bruno1996/countitem:1.0"
    ram: 256
# execution:
#   engine: "ccfaice"
#   settings: {}
execution:
  engine: "ccagency"
  settings:
    access:
      url: "https://souvemed-agency.f4.htw-berlin.de:443/cc"
      auth:
        username: "Bruno"
        password: "{{agency_htw_souvemed_password}}"
