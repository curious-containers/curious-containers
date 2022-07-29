FROM ubuntu:focal

# echo timezone information for uwsgi-plugin-python3
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt-get -y install python3-pip python3-venv uwsgi uwsgi-plugin-python3 # apache2 libapache2-mod-proxy-uwsgi

# create opt directories
RUN useradd -ms /bin/bash cc && mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown cc:cc /opt/ccagency/unprivileged

ADD --chown=root:root ccagency-broker.ini /opt/ccagency/privileged/ccagency-broker.ini

USER cc

# install pyzmq beforehand, otherwise agency installation does not work, downgrade pyzmq
ADD requirements.txt /home/cc/requirements.txt
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate && python3 -m pip install wheel && python3 -m pip install --no-deps -r /home/cc/requirements.txt

# add config file
RUN mkdir -p /home/cc/.config
ADD --chown=cc:cc cc-agency.yml /home/cc/.config/cc-agency.yml

ENV VIRTUAL_ENV=/home/cc/ccagency-venv
