#!/bin/bash
if [ $# -ge 1 ] 
then
sed -i "/mock.date/c mock.date: $1" /opt/module/applog/application.yml
fi
cd /opt/module/applog;java -jar gmall2020-mock-log-2021-11-29.jar >/dev/null 2>&1 &
