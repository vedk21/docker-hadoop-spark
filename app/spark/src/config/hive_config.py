HIVE_SERVER_CONFIG = {
  'host': 'hive-server',
  'port': 10000,
  'warehouse_location': '/user/hive/warehouse'
  # Username and password are not created yet
}

HIVE_METASTORE_CONFIG = {
  'host': 'hive-metastore',
  'port': 9083
}

HIVE_DATABASE_CONFIG = {
  'DATABASE_NAME': 'video_games_analysis',
  'BASIC_DETAILS_TABLE': 'games_details',
  'BASIC_ATTRIBUTES_TABLE': 'games_attributes',
}