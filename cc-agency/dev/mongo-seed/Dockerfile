FROM docker.io/mongo:4.0

RUN apt-get update \
&& apt-get install -y python3-pip \
&& pip3 install --user ruamel.yaml==0.15.*

ENV PYTHONPATH /opt/cc-agency
