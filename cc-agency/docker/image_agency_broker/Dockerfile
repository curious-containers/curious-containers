FROM ubuntu:focal

# install timezone information, python and uwsgi
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt-get -y install python3-pip python3-venv uwsgi uwsgi-plugin-python3

# create opt directories
RUN useradd -ms /bin/bash cc && mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown cc:cc /opt/ccagency/unprivileged

ADD --chown=root:root ccagency-broker.ini /opt/ccagency/privileged/ccagency-broker.ini

USER cc

# install agency
ADD requirements.txt /home/cc/requirements.txt
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate && python3 -m pip install wheel && python3 -m pip install --no-deps -r /home/cc/requirements.txt

# add config file
RUN mkdir -p /home/cc/.config
ADD --chown=cc:cc cc-agency.yml /home/cc/.config/cc-agency.yml

# install startup script
ADD --chown=cc:cc create_broker_user.py /home/cc/create_broker_user.py
ADD --chown=cc:cc start_broker.sh /home/cc/start_broker.sh

RUN chmod u+x /home/cc/start_broker.sh /home/cc/create_broker_user.py

ENV VIRTUAL_ENV=/home/cc/ccagency-venv
