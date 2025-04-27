#!/bin/bash

# 要添加到 /etc/hosts 的容器名称列表（用空格分隔）
CONTAINERS=("namenode" "datanode" "hive-metastore" "hive-server" "spark-master")

# 标志行（便于脚本再次运行时识别更新内容）
START_MARK="# >>> docker container mappings >>>"
END_MARK="# <<< docker container mappings <<<"

# 临时 hosts 内容
TMP_HOSTS=$(mktemp)

# 备份原始 hosts
sudo cp /etc/hosts /etc/hosts.bak

# 清理旧条目
awk "/$START_MARK/{flag=1;next}/$END_MARK/{flag=0;next}!flag" /etc/hosts > "$TMP_HOSTS"

# 写入标志头
echo "$START_MARK" >> "$TMP_HOSTS"

# 添加新的容器 IP 映射
for cname in "${CONTAINERS[@]}"; do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$cname" 2>/dev/null)
  if [[ -n "$ip" ]]; then
    echo "$ip    $cname" >> "$TMP_HOSTS"
  else
    echo "Warning: container [$cname] not found or not running."
  fi
done

# 写入标志尾
echo "$END_MARK" >> "$TMP_HOSTS"

# 替换 hosts 文件（需要 sudo）
sudo cp "$TMP_HOSTS" /etc/hosts
rm "$TMP_HOSTS"

echo "/etc/hosts 已更新完成。"

