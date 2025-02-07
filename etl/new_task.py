def new_task_function():

    print("This is a new task")
    # Import necessary libraries
    from pyspark.sql import SparkSession
  
    import os

    # Load environment variables


    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Snowflake to PostgreSQL") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3,org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    # Define Snowflake options
    snowflake_options = {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv('SNOWFLAKE_USER'),
        "sfPassword": os.getenv('SNOWFLAKE_PASSWORD'),
        "sfDatabase": os.getenv('SNOWFLAKE_DATABASE'),
        "sfSchema": os.getenv('SNOWFLAKE_SCHEMA'),
        "sfWarehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "sfRole": os.getenv('SNOWFLAKE_ROLE')
    }

    def load_tables(snowflake_options: dict):
        # Load data from Snowflake
        df = spark.read \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "Sheet1") \
            .load()
        return df

    def load_from_snowflake_to_postgresql(snowflake_options: dict, pg_url: str, pg_properties: dict):
        joined_df = load_tables(snowflake_options)
        
        # Ensure there are no empty column names and no duplicate column names
        joined_df = joined_df.toDF(*[col.replace(' ', '_').replace('"', '').replace('-', '_') if col else f"col_{i}" for i, col in enumerate(joined_df.columns)])
        joined_df = joined_df.toDF(*[f"{col}_{i}" if joined_df.columns.count(col) > 1 else col for i, col in enumerate(joined_df.columns)])
        print(joined_df.columns)
        
        # Truncate the existing table in PostgreSQL before loading new data
        from psycopg2 import connect

        conn = None  # Initialize conn to None
        try:
            # Ensure the URL is correctly formatted for psycopg2
            if pg_url.startswith('jdbc:'):
                pg_url = pg_url[5:]
            
            conn = connect(
                dbname=pg_url.split('/')[-1],
                user=pg_properties['user'],
                password=pg_properties['password'],
                host=pg_url.split('/')[2].split(':')[0],
                port=pg_url.split('/')[2].split(':')[1] if ':' in pg_url.split('/')[2] else '5432'
            )
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE AdventureWorks RESTART IDENTITY CASCADE")
                conn.commit()
                print("AdventureWorks table truncated successfully.")
        except Exception as e:
            print(f"Failed to truncate table AdventureWorks: {e}")
        finally:
            if conn:
                conn.close()
        
        # Ensure the URL is correctly formatted for Spark JDBC
        jdbc_url = f"jdbc:postgresql://{pg_url.split('//')[1]}"

        # Write joined data to PostgreSQL
        if joined_df:
            joined_df.write \
                .jdbc(url=jdbc_url, table="AdventureWorks", mode="overwrite", properties=pg_properties)
            print("Joined data written to PostgreSQL")
        else:
            print("No sheets could be joined due to missing common columns.")

    # Example usage
    load_from_snowflake_to_postgresql(snowflake_options, os.getenv('POSTGRESQL_URL'), {
        'user': os.getenv('POSTGRESQL_USER'),
        'password': os.getenv('POSTGRESQL_PASSWORD'),
        'driver': os.getenv('POSTGRESQL_DRIVER'),
        'currentSchema': os.getenv('POSTGRESQL_SCHEMA')
    })

    print('Joined data loaded from Snowflake and written to PostgreSQL successfully.')

# new_task_function()