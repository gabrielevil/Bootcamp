import snowflake.connector
import pandas as pd

#Establish connections to source and target databases
passw = ''
my_db_conn = snowflake.connector.connect(
    user = 'GABRIELEV',
    password = passw,
    account = 'VWXTWNV-ON75829',
    warehouse = 'COMPUTE_WH',
    database = 'MY_DB',
    schema = 'PUBLIC'
)

covid_conn = snowflake.connector.connect(
    user = 'GABRIELEV',
    password = passw,
    account = 'VWXTWNV-ON75829',
    warehouse = 'COMPUTE_WH',
    database = 'COVID19_EPIDEMIOLOGICAL_DATA',
)

# Create cursors for both connections
my_db_cursor = my_db_conn.cursor()
covid_cursor = covid_conn.cursor()

# Extract data from source database (shared)
my_db_query = '''SELECT * FROM HAPPINESS_TABLE 
                WHERE INDEX IS NOT NULL 
                AND YEAR > 2019
                ORDER BY YEAR DESC, RANK ASC 
                '''
my_db_cursor.execute(my_db_query)
my_db_data = my_db_cursor.fetchall()


# Extract data from other database
covid_query = '''SELECT 
                    COUNTRY_REGION AS COUNTRY,
                    EXTRACT(YEAR FROM DATE) AS YEAR,
                    SUM(CASES_WEEKLY) AS CASES,
                    SUM(DEATHS_WEEKLY) AS DEATHS
                FROM ECDC_GLOBAL_WEEKLY
                GROUP BY YEAR, COUNTRY
                ORDER BY YEAR DESC, COUNTRY
                '''
covid_cursor.execute(covid_query)
eu_covid_data = covid_cursor.fetchall()



#Merge data
my_db_pd = pd.DataFrame(my_db_data, columns = ['COUNTRY', 'YEAR', 'INDEX', 'RANK']) 
eu_covid_pd = pd.DataFrame(eu_covid_data, columns = ['COUNTRY', 'YEAR', 'CASES', 'DEATHS']) 


#Convert data types in DataFrames
eu_covid_pd['YEAR'] = eu_covid_pd['YEAR'].astype(int)
my_db_pd['YEAR'] = my_db_pd['YEAR'].astype(int)

#Merge data
covid_on_happiness = pd.merge(eu_covid_pd, my_db_pd, on = ['COUNTRY', 'YEAR'], how = 'left')

#Creating a table in Snowflake to upload the merged data into
new_table_query = f'''
CREATE OR REPLACE TABLE COVID19_ON_HAPPINESS (
    COUNTRY varchar,
    YEAR int,
    CASES int,
    DEATHS int,
    INDEX float,
    RANK int
);
'''
my_db_cursor.execute(new_table_query)

# Commit the table creation
my_db_conn.commit()

#convert dataframe to csv
csv_table = covid_on_happiness.to_csv('covid_happiness.csv', index=False)

#Put the csv into snowflake
put_query = f"PUT file:///home/bootcamp/gabriele/Project/covid_happiness.csv @MY_STAGE AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=NONE;"
my_db_cursor.execute(put_query)
my_db_conn.commit()

#copy the csv to a table in snowflake
copy_query = f'''
COPY INTO COVID19_ON_HAPPINESS
FROM '@my_stage/covid_happiness.csv' 
FILE_FORMAT = MY_CSV
PURGE = TRUE
ON_ERROR = CONTINUE;
'''
my_db_cursor.execute(copy_query)
my_db_conn.commit()

#Close cursors and connections
my_db_cursor.close()
covid_cursor.close()
my_db_conn.close()
covid_conn.close()