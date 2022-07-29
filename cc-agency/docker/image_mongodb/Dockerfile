FROM mongo

ADD mongo-init.js /docker-entrypoint-initdb.d/mongo-init.js
RUN chmod u+x /docker-entrypoint-initdb.d/mongo-init.js
