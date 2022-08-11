FROM ubuntu:focal

# install timezone information
RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
# python/uwsgi installation
RUN apt-get -y install python3-pip python3-venv uwsgi uwsgi-plugin-python3

# create cc user and opt directories
RUN useradd -ms /bin/bash cc
RUN mkdir -p /opt/ccagency/privileged /opt/ccagency/unprivileged && chown www-data:www-data /opt/ccagency/unprivileged

ADD --chown=root:root ccagency-trustee.ini /opt/ccagency/privileged/ccagency-trustee.ini

USER cc

# install agency via requirements.txt, as direct installation does not work at the moment
ADD requirements.txt /home/cc/requirements.txt
RUN python3 -m venv /home/cc/ccagency-venv && . /home/cc/ccagency-venv/bin/activate && python3 -m pip install --no-deps -r /home/cc/requirements.txt

# add config file
RUN mkdir -p /home/cc/.config
ADD --chown=cc:cc cc-agency.yml /home/cc/.config/cc-agency.yml

ENV VIRTUAL_ENV=/home/cc/ccagency-venv
