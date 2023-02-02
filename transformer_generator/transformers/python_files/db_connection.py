import configparser
import os
from sqlalchemy import create_engine
from urllib.parse import quote

configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

port = config['CREDs']['db_port']
host = config['CREDs']['db_host']
user = config['CREDs']['db_user']
password = config['CREDs']['db_password']
database = config['CREDs']['database']
def db_connection():
    engine='postgresql://'+user+':%s@'+host+':'+port+'/'+database
    con=create_engine(engine %quote(password))
    cur = con.connect()
    return con ,cur