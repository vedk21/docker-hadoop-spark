FROM bde2020/spark-worker:2.4.3-hadoop2.7
MAINTAINER Vedant Kakade <vedantkakade.cp@gmail.com>

# Set user to root user
USER root

# Install required packages
RUN apk add --no-cache vim

WORKDIR /home

COPY src/config /home/config
COPY src/transform /home/transform

ENV SPARK_HOME=/spark
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
ENV PATH=$PATH:/spark/bin

COPY src/libs/mysql-connector-java-8.0.26.jar $SPARK_HOME/jars/
COPY src/libs/mongo-spark-connector_2.11-2.4.2.jar $SPARK_HOME/jars/
COPY src/libs/mongodb-driver-3.8.1.jar $SPARK_HOME/jars/
COPY src/libs/mongodb-driver-core-3.8.1.jar $SPARK_HOME/jars/
COPY src/libs/bson-3.8.1.jar $SPARK_HOME/jars/
