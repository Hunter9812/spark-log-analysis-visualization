#!/bin/bash

# Check argument
if [ $# -ne 1 ]; then
  echo "Usage: $0 <json-file or directory>"
  exit 1
fi

INPUT=$1
FAILED_FILES=()

send_json_file() {
  local file=$1
  local template_name=$(basename "$file" .json)

  RESPONSE=$(curl -s -o /tmp/curl_output.json -w "%{http_code}" -X PUT "http://localhost:9200/_template/$template_name" \
    -H "Content-Type: application/json" \
    -d @"$file")

  if [ "$RESPONSE" -ne 200 ] || ! grep -q '"acknowledged":true' /tmp/curl_output.json; then
    FAILED_FILES+=("$file")
  fi
}

# Process file or directory
if [ -f "$INPUT" ]; then
  if [[ "$INPUT" == *.json ]]; then
    send_json_file "$INPUT"
  else
    echo "Error: Only .json files are supported"
    exit 1
  fi
elif [ -d "$INPUT" ]; then
  for file in "$INPUT"/*.json; do
    if [ -f "$file" ]; then
      send_json_file "$file"
    fi
  done
else
  echo "Error: Path not found: $INPUT"
  exit 1
fi

# Output failed files
if [ ${#FAILED_FILES[@]} -ne 0 ]; then
  echo -e "\nThe following files failed to upload:"
  for f in "${FAILED_FILES[@]}"; do
    echo "$f"
  done
  exit 1
else
  echo -e "\nAll files uploaded successfully."
  exit 0
fi
