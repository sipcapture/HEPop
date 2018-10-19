#!/bin/bash

if [ -n "$REBUILD" ]; then 
  echo "Rebuilding from GIT master..."
  cd /app && git reset --hard origin/master && git pull && npm install && cd -; 
fi

exec "$@"
