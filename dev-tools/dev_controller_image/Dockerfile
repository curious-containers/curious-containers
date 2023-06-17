FROM bruno1996/cc-agency-controller:0.1

ARG DOCKER_GID

USER root

RUN groupdel docker || true && groupadd --gid $DOCKER_GID docker && usermod -aG docker cc && newgrp docker

USER cc

# install new agency version from wheels
RUN mkdir -p /home/cc/wheels
ADD --chown=cc:cc *.whl /home/cc/wheels/
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate \
	&& python3 -m pip install -q --disable-pip-version-check --force-reinstall /home/cc/wheels/cc_agency*.whl /home/cc/wheels/cc_core*.whl /home/cc/wheels/red_val*.whl
