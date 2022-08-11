FROM ubuntu:focal

RUN apt-get update && apt-get -y install python3-pip python3-venv

# create docker group and opt directories
RUN useradd -ms /bin/bash cc && groupadd --gid 966 docker && usermod -aG docker cc && newgrp docker && newgrp cc
RUN mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown cc:cc /opt/ccagency/unprivileged

USER cc

# install agency via requirements.txt, as direct installation does not work at the moment
ADD requirements.txt /home/cc/requirements.txt
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate && python3 -m pip install --no-deps -r /home/cc/requirements.txt

# add config file
RUN mkdir -p /home/cc/.config
ADD --chown=cc:cc cc-agency.yml /home/cc/.config/cc-agency.yml
