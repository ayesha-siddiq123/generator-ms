import os
import configparser
import pandas as pd
from urllib.parse import quote
from sqlalchemy import create_engine


configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path);

port = config['CREDs']['port']
host = config['CREDs']['host']
user = config['CREDs']['user']
password = config['CREDs']['password']
database = config['CREDs']['database']


engine='postgresql://'+user+':%s@'+host+':'+port+'/'+database
con=create_engine(engine %quote(password))
cur = con.connect()

def Datainsert(valueCols={ValueCols}):
    df_dataset=pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/events/" + {KeyFile})
    {DatasetCasting}
    col_list = df_dataset.columns
    df_snap = df_dataset[col_list]
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in col_list:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values});'''\
            .format(','.join(map(str,values)))
            cur.execute(query)
    except Exception as error:
        print(error)

Datainsert()

















