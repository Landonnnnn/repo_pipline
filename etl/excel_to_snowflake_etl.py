def excel_to_snowflake_etl(): 
    
    # Import necessary libraries
    from pyspark.sql import SparkSession
    import pandas as pd
    
    import os
    import requests
    from io import BytesIO
    # Load environment variables
  
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Snowflake to PostgreSQL") \
        .master("local[*]") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3,com.crealytics:spark-excel_2.12:0.13.5") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Define Snowflake options
    snowflake_options = {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv('SNOWFLAKE_USER'),
        "sfPassword": os.getenv('SNOWFLAKE_PASSWORD'),
        "sfDatabase": os.getenv('SNOWFLAKE_DATABASE'),
        "sfSchema": os.getenv('SNOWFLAKE_SCHEMA'),
        "sfRole": os.getenv('SNOWFLAKE_ROLE')
    }
    
    # Function to load all sheets from an Excel file and write them to Snowflake
    def load_and_write_excel_to_snowflake(snowflake_options: dict):
        github_url = "https://github.com/Landonnnnn/repo_pipline/raw/refs/heads/main/fleet_service_data.xlsx"  # Replace with the actual raw URL

        # Step 1: Download the Excel file from GitHub
        response = requests.get(github_url)
        if response.status_code == 200:
            print("File downloaded successfully!!!")
        else:
            raise Exception(f"Failed to download file from GitHub. Status code: {response.status_code}")

        # Step 2: Read the Excel file into a Pandas DataFrame
        excel_file = BytesIO(response.content)  # Treat the content as a file
        print("File in")
        # sheets = pd.ExcelFile(excel_file)  
        # Step 1: Get all sheet names using Pandas
        excel_file = pd.ExcelFile(excel_file)
        sheet_names = excel_file.sheet_names
        

        # Step 2: Load each sheet into a Spark DataFrame
        spark_dfs = {}
        for sheet_name in sheet_names:
            print(f"Loading sheet: {sheet_name}")
            spark_df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("dataAddress", f"'{sheet_name}'!A1") \
                .option("maxRowsInMemory", 20000) \
                .load(github_url)
            for col in spark_df.columns:
                spark_df = spark_df.withColumnRenamed(col, col.replace(' ', '_'))
            # Add the DataFrame to a dictionary with the sheet name as the key
            spark_dfs[sheet_name] = spark_df
            print(f"Loaded {sheet_name} with {spark_df.count()} rows")
            
            # Define Snowflake table name (based on sheet name)
            table_name = sheet_name.replace(" ", "_")
            spark_df.show()
            print(snowflake_options,table_name)
            # Write data to Snowflake
            spark_df.write \
                    .format("snowflake") \
                    .options(**snowflake_options) \
                    .option("dbtable", table_name) \
                    .mode("overwrite") \
                    .save()
                
            print(f"Data written to Snowflake table '{table_name}'")

    # Load and write the AdventureWorks data from an Excel file to Snowflake
    # excel_file_path = excel_path  # Use raw string
    load_and_write_excel_to_snowflake(snowflake_options)

    spark.stop()
    
# excel_to_snowflake_etl()

