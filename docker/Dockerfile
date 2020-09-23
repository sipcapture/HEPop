# HEPOP-SERVER
FROM node:8

# BUILD FORCE
ENV BUILD 703089

# Requires HOMER7-UI Git
COPY . /app
WORKDIR /app
RUN npm install

# Expose Ports
EXPOSE 9060
EXPOSE 8080

# Configure entrypoint
COPY ./docker/docker-entrypoint.sh /
COPY ./docker/docker-entrypoint.d/* /docker-entrypoint.d/
RUN chmod +x /docker-entrypoint.d/* /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [ "npm", "start" ]
