from flask import Blueprint, request, abort, make_response, jsonify
import json
import datetime

# Make the Snowflake connection
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session
import snowflake.snowpark.functions as f
from config import creds
def connect() -> Session:
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
    return Session.builder.configs(creds).create()

session = connect()

# Make the API endpoints
snowpark = Blueprint('snowpark', __name__)


@snowpark.route('/countries/top10')
def countries_top10():
    try:
        df = session.table('my_db.public.covid19_on_happiness') \
            .filter(f.col('index').isNotNull()) \
            .group_by(f.col('country')) \
            .agg(
                f.avg(f.col('index')).alias('avg_happiness'),
                f.avg(f.col('cases')).alias('avg_covid_cases'),
                f.avg(f.col('deaths')).alias('avg_covid_deaths')
            ) \
            .sort(f.col('avg_happiness').desc()) \
            .limit(10)
        return make_response(jsonify([x.as_dict() for x in df.to_local_iterator()]))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")


