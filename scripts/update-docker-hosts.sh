#!/bin/bash

# List of container names to add to /etc/hosts (space-separated)
CONTAINERS=("namenode" "datanode" "hive-metastore" "hive-server" "spark-master" "spark-worker-1")

# Marker lines (used to identify and update entries during reruns)
START_MARK="# >>> docker container mappings >>>"
END_MARK="# <<< docker container mappings <<<"

# Temporary hosts file
TMP_HOSTS=$(mktemp)

# Backup original hosts file
sudo cp /etc/hosts /etc/hosts.bak

# Remove old entries between markers
awk "/$START_MARK/{flag=1;next}/$END_MARK/{flag=0;next}!flag" /etc/hosts > "$TMP_HOSTS"

# Add start marker
echo "$START_MARK" >> "$TMP_HOSTS"

# Add custom static mapping
echo "127.0.0.1   bigdata.gmall.com" >> "$TMP_HOSTS"

# Add new container IP mappings
for cname in "${CONTAINERS[@]}"; do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$cname" 2>/dev/null)
  if [[ -n "$ip" ]]; then
    echo "$ip    $cname" >> "$TMP_HOSTS"
  else
    echo "Warning: container [$cname] not found or not running."
  fi
done

# Add end marker
echo "$END_MARK" >> "$TMP_HOSTS"

# Replace hosts file (requires sudo)
sudo cp "$TMP_HOSTS" /etc/hosts
rm "$TMP_HOSTS"

echo "/etc/hosts has been updated."
