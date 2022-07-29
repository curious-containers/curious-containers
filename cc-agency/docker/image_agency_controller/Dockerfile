FROM ubuntu:focal

# mongo client installation
RUN apt-get update && apt install -y wget gnupg && wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add -
RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" > /etc/apt/sources.list.d/mongodb-org-6.0.list
RUN apt-get update && apt-get install -y mongodb-org && ln -s /usr/bin/mongosh /usr/bin/mongo

# install timezone information
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
# python/uwsgi installation
RUN apt-get -y install python3-pip python3-venv uwsgi uwsgi-plugin-python3

# create opt directories
RUN useradd -ms /bin/bash cc && groupadd --gid 966 docker && usermod -aG docker cc && newgrp docker && newgrp cc
RUN mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown cc:cc /opt/ccagency/unprivileged

ADD --chown=root:root ccagency-trustee.ini /opt/ccagency/privileged/ccagency-trustee.ini
ADD --chown=root:root ccagency-broker.ini /opt/ccagency/privileged/ccagency-broker.ini

USER cc

# install agency via requirements.txt, as direct installation does not work at the moment
ADD requirements.txt /home/cc/requirements.txt
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate && python3 -m pip install wheel && python3 -m pip install --no-deps -r /home/cc/requirements.txt

# add config file
RUN mkdir -p /home/cc/.config
ADD --chown=cc:cc cc-agency.yml /home/cc/.config/cc-agency.yml
