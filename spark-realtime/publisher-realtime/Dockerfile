FROM azul/zulu-openjdk:17
EXPOSE 8080
# 设置工作目录
WORKDIR /app
COPY ./target/publisher-realtime-0.0.1-SNAPSHOT.jar ./app.jar
CMD java -jar app.jar
