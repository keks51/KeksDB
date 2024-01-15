FROM openjdk:11

WORKDIR /

RUN mkdir /kv_storage_data

ADD ./target/keks-db-1.0.0.jar keks-db-1.0.0.jar

CMD java -cp keks-db-1.0.0.jar \
com.keks.kv_storage.KVClient
#$KV_SERVER_HOST \
#$KV_SERVER_PORT \
#$RECORDS_NUMBER \
#$NUMBER_OF_THREADS \
#$NUMBER_OF_KEYS_ININDEX \
#$IN_MEMORY_RECORDS \