CREATE OR REPLACE PROCEDURE PRACTICE.EVENT_MGMT.ETL_JOB()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake==1.0.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    table_names = ["category", "date", "venue", "users", "sales", "listing", "event"]
    
    for table_name in table_names:
        stage_path = f"@practice.event_mgmt.stg_{table_name}/"

        # apply error handling using try/except, in case the stage cannot be created or replaced
        try:
            # create or replace stage areas, then load S3 data into them
            session.sql(
                f"""
                CREATE OR REPLACE STAGE practice.event_mgmt.stg_{table_name}
                    STORAGE_INTEGRATION = s3_data_integration
                    URL = ''s3://[BUCKETNAME]/{table_name}/''
                    FILE_FORMAT = (TYPE=PARQUET);
                """
            ).collect()
        except Exception as e:
            print(f"Error creating/replacing stage for {table_name}: {e}")
            continue

        # validate the existence of parquet file(s)
        if has_parquet_file(session, stage_path):         
            # read parquet data from stage into a DataFrame
            df = session.read.parquet(stage_path)    
    
            # remove double-quotes (") from column names
            df_prepared = df.select([col(c).alias(c.strip(''"'')) for c in df.columns])
                
            # save dataframe as a table
            df_prepared.write.mode("overwrite").save_as_table(f"practice.event_mgmt.{table_name}")
        else:
            print(f"No parquet file(s) found in {stage_path}. Please check source system.")
   
    return "End of ETL"

def has_parquet_file(session: snowpark.Session, stage_path: str) -> bool: 
    # apply error handling using try/except, in case the stage doesn''t exist or is inaccessible
    try:
        # attempt to list files in the specified stage
        result = session.sql(f"LIST {stage_path};").collect()
    
        # check if any files have the ".parquet" extension
        has_parquet = any(row.name.endswith(''.parquet'') for row in result)   
        return has_parquet   
    except Exception as e:
        print(f"Error accessing stage {stage_path}: {e}")
        return False';