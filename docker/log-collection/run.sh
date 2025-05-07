#!/bin/bash

# Check the number of arguments
if [ $# -lt 1 ]; then
  echo "Usage: $0 [app|db] [mock.date]"
  exit 1
fi

SERVICE=$1
MOCK_DATE=$2

case "$SERVICE" in
  app)
    cd applog || { echo "Failed to change to directory 'applog'"; exit 1; }
    if [ -n "$MOCK_DATE" ]; then
      sed -i "/mock.date/c mock.date: $MOCK_DATE" application.yml
    fi
    java -jar gmall2020-mock-log-2021-11-29.jar
    ;;
  db)
    cd db_log || { echo "Failed to change to directory 'db_log'"; exit 1; }
    if [ -n "$MOCK_DATE" ]; then
      sed -i "/mock.date/c mock.date: $MOCK_DATE" application.properties
    fi
    java -jar gmall2020-mock-db-2021-01-22.jar
    ;;
  *)
    echo "Invalid service type: $SERVICE. Must be 'app' or 'db'."
    exit 1
    ;;
esac
