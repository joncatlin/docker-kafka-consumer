############################################################

# Set the base image to Java 1.8
FROM openjdk:8-jre-alpine

# File Author / Maintainer
MAINTAINER Jon Catlin

################## BEGIN INSTALLATION ######################
COPY /kafka-consumer-1.0.6.jar /home
COPY dependency /home/dependency

ENV KAFKA_NODES server.domain.com:9092
ENV COUCHBASE_NODES server.domain.com
ENV KAFKA_TOPIC topic
ENV KAFKA_GROUP_ID group_id
ENV STATSD_SERVER server.domain.com
ENV BATCH_SIZE 1

##################### INSTALLATION END #####################

# Set default container command
ENTRYPOINT java -classpath /home/kafka-consumer-1.0.6.jar:/home/dependency/* ConsumerWriteDB
