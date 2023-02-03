import configparser
import os
from urllib.parse import quote
import psycopg2 as pg

configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path);

port = config['CREDs']['db_port']
host = config['CREDs']['db_host']
user = config['CREDs']['db_user']
password = config['CREDs']['db_password']
database = config['CREDs']['database']

def db_connection():
    con = pg.connect(database=database, user=user, password=password, host=host, port=port)
    cur = con.cursor()
    return con,cur