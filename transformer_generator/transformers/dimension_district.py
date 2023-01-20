import os
import configparser
import pandas as pd
from urllib.parse import quote
from sqlalchemy import create_engine


configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path);

port = config['CREDs']['db_port']
host = config['CREDs']['db_host']
user = config['CREDs']['db_user']
password = config['CREDs']['db_password']
database = config['CREDs']['Database']


engine='postgresql://'+user+':%s@'+host+':'+port+'/'+database
con=create_engine(engine %quote(password))
cur = con.connect()

def Datainsert(valueCols=['district_id', 'district_name', 'lattitude', 'longitude']):
    df_data=pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/events/" + "dimension_district.csv")
    df_data.update(df_data[["lattitude", "longitude"]].applymap("'{}'".format))
    df_snap = df_data[valueCols]
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in valueCols:
              values.append(row[i])
            query = ''' INSERT INTO ingestion.dimension_district(district_id,district_name,lattitude,longitude) VALUES ({});'''\
            .format(','.join(map(str,values)))
            cur.execute(query)
    except Exception as error:
        print(error)

Datainsert()

















