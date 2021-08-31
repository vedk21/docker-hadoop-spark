import sys
import os
# Please install mysql-connector-python for this package
import mysql.connector
from mysql.connector import errorcode
# add internal packages
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'config'))
from mysql_config import MYSQL_SERVER_CONFIG

def connect_to_server():
  try:
    mysql_connection = mysql.connector.connect(**MYSQL_SERVER_CONFIG)
    print('Started MySQL Connection')
    return mysql_connection
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
      exit(1)
    else:
      print(err)
      exit(1)

def create_database(db_connection, db_cursor, db_name):
  try:
    db_cursor.execute("CREATE DATABASE {}".format(db_name))
  except mysql.connector.Error as err:
    print("Failed creating database: {}".format(err))
    db_connection.close()
    exit(1)

def create_or_use_database(db_connection, db_cursor, db_name):
  try:
    db_cursor.execute("USE {}".format(db_name))
  except mysql.connector.Error as err:
    print("Database {} does not exists.".format(db_name))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
      create_database(db_connection, db_cursor, db_name)
      print("Database {} created successfully.".format(db_name))
      db_connection.database = db_name
    else:
      print(err)
      db_connection.close()
      exit(1)

def create_or_use_table(db_connection, db_cursor, table_name, create_table_statement):
  try:
    print("Creating table {}: ".format(table_name))
    db_cursor.execute(create_table_statement.format(table_name))
    print("Table {} created successfully.".format(table_name))
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
      print("Table already exists.")
    else:
      print(err.msg)
      db_connection.close()

def insert_data_into_table(db_connection, db_cursor, table_name, insert_into_table_statement, insert_data):
  try:
    db_cursor.executemany(insert_into_table_statement.format(table_name), insert_data)
    # Make sure data is committed to the database
    db_connection.commit()
    print('Data dumped successfully.')
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_DUP_ENTRY:
      print("Duplicate entries. No insert opertion performed")
    else:
      print(err.msg)
      db_connection.close()

def add_games_data_into_mysql(DB_NAME, TABLE_NAME, GAMES_DATA):
  CRAETE_TABLE_STATEMENT = '''
    CREATE TABLE {} (
      id INT,
      name VARCHAR(255),
      slug VARCHAR(255),
      playtime INT,
      released DATE,
      PRIMARY KEY(id)
    )
  '''
  INSERT_INTO_TABLE_STATEMENT = '''
    INSERT INTO {} (id, name, slug, playtime, released)
    VALUES (%(id)s, %(name)s, %(slug)s, %(playtime)s, %(released)s)
  '''

  db_connection = connect_to_server()
  if db_connection:
    db_cursor = db_connection.cursor()
    create_or_use_database(db_connection, db_cursor, DB_NAME)
    create_or_use_table(db_connection, db_cursor, TABLE_NAME, CRAETE_TABLE_STATEMENT)
    insert_data_into_table(db_connection, db_cursor, TABLE_NAME, INSERT_INTO_TABLE_STATEMENT, GAMES_DATA)

    # Close the connection and cursor once done
    db_cursor.close()
    db_connection.close()
