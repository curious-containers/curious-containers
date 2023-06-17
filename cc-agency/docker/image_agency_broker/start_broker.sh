#!/bin/sh

# create broker user
/home/cc/ccagency-venv/bin/python3 /home/cc/create_broker_user.py

# start agency broker
/usr/bin/uwsgi /opt/ccagency/privileged/ccagency-broker.ini
