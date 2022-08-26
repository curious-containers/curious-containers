#!/bin/bash

# patch pyproject files to use path dependencies instead of pip dependencies

set -e

cd curious-containers

for f in $(find -name "pyproject.toml"); do
	sed -i "/^cc-core = /ccc-core = { path = \"../cc-core\", develop = true }" "$f"
	sed -i "/^red-val = /cred-val = { path = \"../red-val\", develop = true }" "$f"
	sed -i "/^red-fill = /cred-fill = { path = \"../red-fill\", develop = true }" "$f"
done
