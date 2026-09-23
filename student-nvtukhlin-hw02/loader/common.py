import os

import psycopg2


def connect():
    return psycopg2.connect(
        host=os.environ["WH_HOST"],
        port=os.environ["WH_PORT"],
        dbname=os.environ["WH_DB"],
        user=os.environ["WH_USER"],
        password=os.environ["WH_PASSWORD"],
    )
