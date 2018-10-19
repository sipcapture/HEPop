# HEPOP-SERVER
FROM node:8

# BUILD FORCE
ENV BUILD 703013

# Requires HOMER7-UI Git
RUN git clone https://github.com/sipcapture/hepop /app
WORKDIR /app
RUN npm install 

# Expose Ports
EXPOSE 9060

# Configure entrypoint
COPY /myconfig.js /app/myconfig.js
COPY /docker-entrypoint.sh /
COPY /docker-entrypoint.d/* /docker-entrypoint.d/
RUN chmod +x /docker-entrypoint.d/* /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [ "npm", "start" ]
