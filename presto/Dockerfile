FROM shawnzhu/prestodb:0.181
MAINTAINER Vedant Kakade <vedantkakade.cp@gmail.com>

ENV PRESTO_HOME=/opt/presto
ADD etc/hive.properties $PRESTO_HOME/etc/

WORKDIR $PRESTO_HOME

# Install presto cli
RUN wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.181/presto-cli-0.181-executable.jar
RUN mv presto-cli-0.181-executable.jar presto
RUN chmod +x presto