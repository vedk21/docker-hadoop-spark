from pyspark.sql import SparkSession
import sys
import os
# add internal packages
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'config'))
from mysql_config import MYSQL_DATABASE_CONFIG, MYSQL_SERVER_CONFIG
from mongo_config import MONGO_DATABASE_CONFIG, MONGO_SERVER_CONFIG
from hive_config import HIVE_METASTORE_CONFIG, HIVE_SERVER_CONFIG, HIVE_DATABASE_CONFIG
from hadoop_config import HADOOP_CONFIG

from clean_and_format_basic_details import clean_and_format_basic_details
# Set this to set default encoding to 'utf-8'
# sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

if __name__ == '__main__':
  warehouse_location = (
    'hdfs://{namenode_host}:{namenode_port}{warehouse_location}'
    .format(
      namenode_host = HADOOP_CONFIG['namenode_host'],
      namenode_port = HADOOP_CONFIG['namenode_port'],
      warehouse_location = HIVE_SERVER_CONFIG['warehouse_location']
    )
  )

  # Mysql DataSource
  spark_mysql = (
    SparkSession
    .builder
    .config('hive.metastore.uris', 'thrift://{hive_metastore}:{port}'
    .format(hive_metastore = HIVE_METASTORE_CONFIG['host'], port = HIVE_METASTORE_CONFIG['port']))
    .config('spark.sql.warehouse.dir', warehouse_location)
    .enableHiveSupport()
    .appName('MysqlDataPreparation')
    .getOrCreate()
  )
  
  # MYSQL connection in spark
  games_basic_details_df = (
    spark_mysql.read.format('jdbc')
    .option('url', 'jdbc:mysql://{hostname}:{port}/{database}'
    .format(database = MYSQL_DATABASE_CONFIG['DATABASE_NAME'],
    hostname = MYSQL_SERVER_CONFIG['host'], port = MYSQL_SERVER_CONFIG['port']))
    .option('driver', 'com.mysql.jdbc.Driver')
    .option('dbtable', MYSQL_DATABASE_CONFIG['TABLE_NAME'])
    .option('user', MYSQL_SERVER_CONFIG['user'])
    .option('password', MYSQL_SERVER_CONFIG['password'])
    .load()
  )

  mysql_formatted_df = clean_and_format_basic_details(games_basic_details_df)

  spark_mysql.sql('CREATE DATABASE {database}'.format(database = HIVE_DATABASE_CONFIG['DATABASE_NAME']))

  (
    mysql_formatted_df
    .write
    .mode('overwrite') # change in future if needed
    .saveAsTable('{database}.{table}'
    .format(database = HIVE_DATABASE_CONFIG['DATABASE_NAME'],table = HIVE_DATABASE_CONFIG['BASIC_DETAILS_TABLE']))
  )

  spark_mysql.stop()

  # MongoDB DataSource 
  spark_mongo = (
    SparkSession
    .builder
    .appName('MongoDataPreparation')
    .getOrCreate()
  )

  # MongoDB connection in spark
  mongo_formatted_df = (
    spark_mongo.read.format('mongo')
    .option('uri', 'mongodb://{username}:{password}@{hostname}:{port}/'
    .format(username = MONGO_SERVER_CONFIG['username'], password = MONGO_SERVER_CONFIG['password'],
    hostname = MONGO_SERVER_CONFIG['host'], port = MONGO_SERVER_CONFIG['port']))
    .option('database', MONGO_DATABASE_CONFIG['DATABASE_NAME'])
    .option('collection', MONGO_DATABASE_CONFIG['COLLECTION_NAME'])
    .load()
  )

  (
    mongo_formatted_df
    .write
    .mode('overwrite') # change in future if needed
    .saveAsTable('{database}.{table}'
    .format(database = HIVE_DATABASE_CONFIG['DATABASE_NAME'],table = HIVE_DATABASE_CONFIG['BASIC_ATTRIBUTES_TABLE']))
  )

  # Stop session
  spark_mongo.stop()
