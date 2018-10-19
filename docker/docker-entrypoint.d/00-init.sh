#!/bin/bash

PGSQL_DBNAME=${PGSQL_DBNAME:-hepic}
PGSQL_TBNAME=${PGSQL_TBNAME:-hep}
PGSQL_HOST=${PGSQL_HOST:-localhost}
PGSQL_PORT=${PGSQL_PORT:-5432}
PGSQL_USER=${PGSQL_USER:-homer_user}
PGSQL_PASSWORD=${PGSQL_PASSWORD:-homer_password}

HEPOP_HOST=${HEPOP_HOST:-127.0.0.1}
HEPOP_PROTO=${HEPOP_PROTO:-udp}
HEPOP_PORT=${HEPOP_PORT:-9060}
HEPOP_ID=${HEPOP_ID:-HEPOP_DEF_001}
HEPOP_DEBUG=${HEPOP_DEBUG:-false}

INFLUXDB_HOST=${INFLUXDB_HOST:-127.0.0.1}
INFLUXDB_PORT=${INFLUXDB_PORT:-8086}
INFLUXDB_DB=${INFLUXDB_DB:-hep}


if [ -f /app/myconfig.js ]; then

   if [ -n "$PGSQL_HOST" ]; then sed -i "s/PGSQL_HOST/${PGSQL_HOST}/g" /app/myconfig.js; fi
   if [ -n "$PGSQL_PORT" ]; then sed -i "s/PGSQL_PORT/${PGSQL_PORT}/g" /app/myconfig.js; fi
   if [ -n "$PGSQL_USER" ]; then sed -i "s/PGSQL_USER/${PGSQL_USER}/g" /app/myconfig.js; fi
   if [ -n "$PGSQL_PASSWORD" ]; then sed -i "s/PGSQL_PASSWORD/${PGSQL_PASSWORD}/g" /app/myconfig.js; fi
   if [ -n "$PGSQL_DBNAME" ]; then sed -i "s/PGSQL_DBNAME/${PGSQL_DBNAME}/g" /app/myconfig.js; fi
   if [ -n "$PGSQL_TBNAME" ]; then sed -i "s/PGSQL_TBNAME/${PGSQL_TBNAME}/g" /app/myconfig.js; fi

   if [ -n "$INFLUXDB_HOST" ]; then sed -i "s/INFLUXDB_HOST/${INFLUXDB_HOST}/g" /app/myconfig.js; fi
   if [ -n "$INFLUXDB_PORT" ]; then sed -i "s/INFLUXDB_PORT/${INFLUXDB_PORT}/g" /app/myconfig.js; fi
   if [ -n "$INFLUXDB_DB" ]; then sed -i "s/INFLUXDB_DB/${INFLUXDB_DB}/g" /app/myconfig.js; fi

   if [ -n "$HEPOP_ID" ]; then sed -i "s/HEPOP_ID/${HEPOP_ID}/g" /app/myconfig.js; fi
   if [ -n "$HEPOP_HOST" ]; then sed -i "s/HEPOP_HOST/${HEPOP_HOST}/g" /app/myconfig.js; fi
   if [ -n "$HEPOP_PORT" ]; then sed -i "s/HEPOP_PORT/${HEPOP_PORT}/g" /app/myconfig.js; fi
   if [ -n "$HEPOP_PROTO" ]; then sed -i "s/HEPOP_PROTO/${HEPOP_PROTO}/g" /app/myconfig.js; fi
   if [ -n "$HEPOP_DEBUG" ]; then sed -i "s/HEPOP_DEBUG/${HEPOP_DEBUG}/g" /app/myconfig.js; fi


   echo "Pre-Flight provisioning completed!"

else

   echo "Configuration file not found!"

fi

exec "$@"
