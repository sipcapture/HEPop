# HEPOP-SERVER
FROM node:8

# BUILD FORCE
ENV BUILD 703061

# Requires HOMER7-UI Git
RUN git clone --single-branch --branch master https://github.com/sipcapture/hepop /app
COPY ./override/src/* /app/src
WORKDIR /app
RUN npm install 

# Expose Ports
EXPOSE 9060
EXPOSE 8080

# Configure entrypoint
COPY /myconfig.js /app/myconfig.js
COPY /docker-entrypoint.sh /
COPY /docker-entrypoint.d/* /docker-entrypoint.d/
RUN chmod +x /docker-entrypoint.d/* /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [ "npm", "start" ]
