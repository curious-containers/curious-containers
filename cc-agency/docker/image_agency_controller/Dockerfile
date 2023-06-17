FROM ubuntu:focal

RUN apt-get update && apt-get -y install python3-pip python3-venv

# create docker group and opt directories
RUN useradd -ms /bin/bash cc && groupadd --gid 966 docker && usermod -aG docker cc && newgrp docker && newgrp cc
RUN mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown cc:cc /opt/ccagency/unprivileged

USER cc

# install agency
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate && python3 -m pip install cc-agency==9.1.1

# add config file
RUN mkdir -p /home/cc/.config
ADD --chown=cc:cc cc-agency.yml /home/cc/.config/cc-agency.yml
