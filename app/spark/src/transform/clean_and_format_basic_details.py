from pyspark.sql.functions import year, month, dayofmonth

def clean_and_format_basic_details(df):
  # Seperate day, month, year from released date
  df = (df.withColumn('released_day', dayofmonth(df.released))
    .withColumn('released_month', month(df.released))
    .withColumn('released_year', year(df.released))
  )
  # Drop released column
  df = df.drop('released')
  # order by released year, released_month, released_day
  return df.orderBy(['released_year', 'released_month', 'released_day'], ascending = False)
  
