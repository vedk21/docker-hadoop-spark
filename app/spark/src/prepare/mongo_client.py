import sys
import os
from pymongo import MongoClient
# add internal packages
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'config'))
from mongo_config import MONGO_SERVER_CONFIG

def create_or_use_database_and_collection(db_connection, db_name, collection_name):
  db = db_connection[db_name]
  cln = db[collection_name]
  # drop collection first
  cln.drop()
  # now create fresh collection
  cln = db[collection_name]
  return cln

def insert_data_into_collection(cln, insert_data):
  cln.insert_many(insert_data)
  print('NOSQL Data inserted Successfully')

def add_games_data_into_mongodb(db_name, collection_name, data):
  db_connection = MongoClient('mongodb://{username}:{password}@{host}:{port}/'.format(username = MONGO_SERVER_CONFIG['username'], password = MONGO_SERVER_CONFIG['password'], host = MONGO_SERVER_CONFIG['host'], port = MONGO_SERVER_CONFIG['port']))
  print('Started MongoDB Connection')
  
  created_collection = create_or_use_database_and_collection(db_connection, db_name, collection_name)

  insert_data_into_collection(created_collection, data)
