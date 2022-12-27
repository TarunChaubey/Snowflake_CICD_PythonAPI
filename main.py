import snowflake.connector

# define paramters for snowflake connection
conn = snowflake.connector.connect(
    user= "tarunchaubey55",
    password= "Bhargav09@",
    account= "fd05331.central-india.azure",
    warehouse='TEST_WAREHOUSES',
    database='SNOWFLAKE_CICD',
    schema= "PUBLIC")

# # define schema for create table in snowflake
sql_query = 'create table stock_prices if not exists( DATE DATE, Open NUMBER(10), High NUMBER(10), Low NUMBER(10), Close NUMBER(10), volume NUMBER(10));'

# # execute query to create table in snowflake
conn.cursor().execute(sql_query)

# # # created intermediate stage in snowflake
conn.cursor().execute("CREATE OR REPLACE STAGE Intermediate_Stage;")

# # # creating format of csv files
conn.cursor().execute("CREATE OR REPLACE FILE FORMAT Intermediate_Stage type ='csv' field_delimiter = '|'")

# # # put files from local system to snowflake stages
conn.cursor().execute("put file://./data/*.csv @Intermediate_Stage;")

# # # copy data from stages to tables
conn.cursor().execute("copy into stock_prices from @Intermediate_Stage/*.csv.gz  on_error = 'CONTINUE'")