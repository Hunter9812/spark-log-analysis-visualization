FROM azul/zulu-openjdk:8

COPY ./applog /opt/module/applog
COPY ./db_log /opt/module/db_log

WORKDIR /opt/module

ADD run.sh ./run.sh
RUN chmod a+x ./run.sh

ENTRYPOINT ["./run.sh"]
