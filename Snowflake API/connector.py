from flask import Blueprint, request, abort, jsonify, make_response
import json
import datetime

# Make the Snowflake connection
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from snowflake.connector import DictCursor
from config import creds
def connect() -> snowflake.connector.SnowflakeConnection:
    if 'private_key' in creds:
        if not isinstance(creds['private_key'], bytes):
            p_key = serialization.load_pem_private_key(
                    creds['private_key'].encode('utf-8'),
                    password=None,
                    backend=default_backend()
                )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption())
            creds['private_key'] = pkb
    return snowflake.connector.connect(**creds)

conn = connect()

# Make the API endpoints
connector = Blueprint('connector', __name__)

## Top 10 customers in date range

@connector.route('/countries/top10')
def countries_top10():
    sql_string = '''
        SELECT
            country,
            AVG(index) AS avg_happiness,
            AVG(cases) AS avg_covid_casesc,
            AVG(deaths) AS avg_covid_deaths
        FROM my_db.public.covid19_on_happiness
        WHERE index IS NOT NULL
        GROUP BY country
        ORDER BY avg_happiness DESC
        LIMIT 10;
        '''
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")


