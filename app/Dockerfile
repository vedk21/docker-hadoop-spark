FROM python:3.7
MAINTAINER Vedant Kakade <vedantkakade.cp@gmail.com>

# Set user to root user
USER root

# Install required packages
RUN apt-get update \
&& apt-get install -y vim \
&& rm -rf /var/lib/apt/lists/*

# Install required python packages
RUN pip install mysql-connector-python pymongo fastparquet requests

WORKDIR /home

COPY spark/src/config /home/config
COPY spark/src/prepare /home/prepare
RUN mkdir /home/data