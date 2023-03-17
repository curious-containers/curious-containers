inputs:
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