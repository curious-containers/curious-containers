FROM ubuntu:focal

RUN apt-get update && apt-get -y install python3-pip python3-venv

# create docker group and opt directories
RUN useradd -ms /bin/bash cc && groupadd --gid 999 docker && usermod -aG docker cc && newgrp docker && newgrp cc
RUN mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown cc:cc /opt/ccagency/unprivileged

USER cc

# install agency
COPY . /home/cc/

RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate
RUN pip install poetry

WORKDIR /home/cc/cc-agency
RUN . /home/cc/ccagency-venv/bin/activate && /home/cc/.local/bin/poetry install

# add config file
RUN mkdir -p /home/cc/.config
