FROM openjdk:11

WORKDIR /

RUN mkdir /keks-db

ADD ./target/keks-db-1.0.0.jar keks-db-1.0.0.jar

CMD java -jar \
-XX:MaxRAMPercentage=80 \
-Dcom.sun.management.jmxremote=true \
-Dcom.sun.management.jmxremote.port=9010 \
-Dcom.sun.management.jmxremote.local.only=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.rmi.port=9010 \
-Djava.rmi.server.hostname=localhost \
keks-db-1.0.0.jar
#$KV_STORAGE_PATH \
#$KV_HTTP_SERVER_PORT \
#$KV_THRIFT_SERVER_PORT \
#$DEFAULT_MAX_NUMBER_OF_RECORDS_IN_MEMORY \
#$DEFAULT_NUMBER_OF_KEYS_IN_BLOCK_INDEX

#CMD java -jar \
#-Dcom.sun.management.jmxremote \
#-Dcom.sun.management.jmxremote.port=9010 \
#-Dcom.sun.management.jmxremote.local.only=false \
#-Dcom.sun.management.jmxremote.authenticate=false \
#-Dcom.sun.management.jmxremote.ssl=false \
#keks-db-1.0.0.jar \
#$KV_STORAGE_PATH \
#$KV_SERVER_PORT \
#$DEFAULT_MAX_NUMBER_OF_RECORDS_IN_MEMORY \
#$DEFAULT_NUMBER_OF_KEYS_IN_BLOCK_INDEX
