#!/bin/bash

set -e

# check right directory
if [ "$(basename $PWD)" = "dev-tools" ]; then
	cd ..
fi

if [ "$(basename $PWD)" != "curious-containers" ]; then
	echo "ERROR: current directory is unknown. Execute in directory \"dev-tools/\" or \"curious-containers/\""
	exit 1
fi

# build wheels for cc-agency cc-core and red-val and copy to dev-tools/dev_controller_image and dev-tools/dev_broker_images
echo "- building wheels for"

for package in "cc-agency" "cc-core" "red-val"; do
	echo "  - $package"
	package_=${package/-/_}
	cd "$package"

	# build wheel
	poetry build -f wheel -n
	package_version="$(poetry version --short -n)"

	# copy to image directories
	cp "dist/$package_-$package_version-py3-none-any.whl" "../dev-tools/dev_broker_image/$package_-$package_version-py3-none-any.whl"
	cp "dist/$package_-$package_version-py3-none-any.whl" "../dev-tools/dev_controller_image/$package_-$package_version-py3-none-any.whl"
	cd ..
done

echo "- building temporary docker images"

# build dev broker images
echo "  - broker"
cd dev-tools/dev_broker_image
docker build --no-cache -t cc-agency-broker-tmp:0.0 .
rm -v ./*.whl  # remove wheels for future attempts

# build dev controller images
echo "  - controller"
cd ../dev_controller_image
docker build --no-cache -t cc-agency-controller-tmp:0.0 .
rm -v ./*.whl  # remove wheels for future attempts
