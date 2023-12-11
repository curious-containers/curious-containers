FROM ubuntu:focal

# install timezone information
RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
# python/uwsgi installation
RUN apt-get -y install python3-pip python3-venv uwsgi uwsgi-plugin-python3

# create cc user and opt directories
RUN useradd -ms /bin/bash cc
RUN mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown www-data:www-data /opt/ccagency/unprivileged

COPY . /home/cc/
RUN cp /home/cc/tests/setup/image_agency_trustee/ccagency-trustee.ini /opt/ccagency/privileged/ccagency-trustee.ini

USER cc

# install agency
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate
RUN pip install poetry

WORKDIR /home/cc/cc-agency
RUN . /home/cc/ccagency-venv/bin/activate && /home/cc/.local/bin/poetry install

# add config file
RUN mkdir -p /home/cc/.config

ENV VIRTUAL_ENV=/home/cc/ccagency-venv
