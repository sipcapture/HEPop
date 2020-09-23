# HEPOP-SERVER
FROM node:8

# BUILD FORCE
ENV BUILD 703089

COPY . /app
WORKDIR /app

# Configure entrypoint
RUN rm -rf docker
COPY ./docker/docker-entrypoint.sh /
COPY ./docker/docker-entrypoint.d/* /docker-entrypoint.d/
RUN chmod +x /docker-entrypoint.d/* /docker-entrypoint.sh

# Expose Ports
EXPOSE 9060
EXPOSE 8080

RUN npm install

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [ "npm", "start" ]
