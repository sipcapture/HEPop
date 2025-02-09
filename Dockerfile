FROM oven/bun:1 AS build
WORKDIR /app
COPY . .
RUN bun install
RUN cp patches/bun_duckdb.js node_modules/@duckdb/node-bindings/duckdb.js
RUN bun build ./hepop.js --compile --outfile /app/hepop
RUN chmod +x /app/hepop

FROM gcr.io/distroless/cc
WORKDIR /app
COPY --from=build /app/hepop /app/hepop
EXPOSE 9069/udp
EXPOSE 9069/tcp
CMD [ "./hepop" ]
