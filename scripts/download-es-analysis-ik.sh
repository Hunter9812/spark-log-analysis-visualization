#!/bin/bash

cd "$(dirname "$0")"

mkdir -p ../docker/elastic/es-plugins/ik
curl -L -o ik.zip https://get.infini.cloud/elasticsearch/analysis-ik/8.17.4
unzip ik.zip -d ../docker/elastic/es-plugins/ik
rm ik.zip
