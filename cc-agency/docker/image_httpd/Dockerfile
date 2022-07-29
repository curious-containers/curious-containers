FROM ubuntu:focal

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata && apt-get -y install apache2 libapache2-mod-proxy-uwsgi

ADD --chown=root:root ccagency-broker.conf /etc/apache2/sites-available/ccagency-broker.conf

# RUN a2enmod ssl && a2enmod proxy_uwsgi && a2ensite ccagency-broker
RUN a2enmod proxy_uwsgi && a2ensite ccagency-broker
