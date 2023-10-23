FROM ubuntu:focal

# install timezone information, python and uwsgi
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt-get -y install python3-pip python3-venv uwsgi uwsgi-plugin-python3

# create opt directories
RUN useradd -ms /bin/bash cc && mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown cc:cc /opt/ccagency/unprivileged

COPY . /home/cc/
RUN cp /home/cc/tests/setup/image_agency_broker/ccagency-broker.ini /opt/ccagency/privileged/ccagency-broker.ini

USER cc

# install agency
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate
RUN pip install poetry

WORKDIR /home/cc/cc-agency
RUN . /home/cc/ccagency-venv/bin/activate && /home/cc/.local/bin/poetry install

# RUN cp /home/cc/tests/setup/cc.pth /home/cc/ccagency-venv/lib/python3.8/site-packages/cc.pth

# add config file
RUN mkdir -p /home/cc/.config

# install startup script
RUN cp /home/cc/cc-agency/docker/image_agency_broker/create_broker_user.py /home/cc/create_broker_user.py
RUN cp /home/cc/cc-agency/docker/image_agency_broker/start_broker.sh /home/cc/start_broker.sh

RUN chmod u+x /home/cc/start_broker.sh /home/cc/create_broker_user.py

ENV VIRTUAL_ENV=/home/cc/ccagency-venv
