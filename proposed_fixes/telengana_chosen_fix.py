df = spark.table("weather_catalog.weather_schema.weather_telangana").limit(10)
display(df)