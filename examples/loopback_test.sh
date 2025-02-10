#!/bin/bash

# Function to generate a random temperature between 60 and 100
generate_temp() {
  echo $((60 + RANDOM % 41))
}

# Database and API configuration
DB_URL="localhost:9070/write"
DB_NAME="sensors"
PRECISION="auto"
ACCEPT_PARTIAL="false"
QUERY_URL="http://localhost:9070/query"

# Room name (can be modified as needed)
room="Garden"

# Get the start time in ISO format
start_time=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo "Script started at: $start_time"

# Counter for tracking every 10 inserts
counter=0

# Infinite loop to send data every second
while true; do
  # Generate a random temperature
  temp=$(generate_temp)

  # Prepare the data payload
  data_payload="home,room=$room temp=$temp"

  # Send the data using curl
  curl -s -XPOST "$DB_URL?db=$DB_NAME&precision=$PRECISION&accept_partial=$ACCEPT_PARTIAL" \
       --data-raw "$data_payload"

  # Print the payload for debugging
  # echo "Payload sent: $data_payload"

  # Increment counter
  ((counter++))

  # Every 10 inserts, check the total count
  if (( counter % 50 == 0 )); then
    query_payload=$(jq -n --arg time "$start_time" '{query: "SELECT count() as count, avg(temp) as temp FROM home WHERE time >= '\''\($time)'\'' LIMIT 1"}')
    response=$(curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" -d "$query_payload")
    echo "Total count: $response"
    echo "Total sent: $counter"
  fi

  # Wait for 1 second before sending the next request
  read -t 0.5

done
