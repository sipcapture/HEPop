FROM oven/bun:1 AS build
WORKDIR /app
COPY . .
RUN bun install
EXPOSE 9069/udp
EXPOSE 9069/tcp
EXPOSE 9070
CMD [ "bun", "./hepop.js" ]
