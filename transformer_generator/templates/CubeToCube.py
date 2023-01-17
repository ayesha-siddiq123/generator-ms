import configparser
import os
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote

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

def aggTransformer(valueCols={ValueCols}):
    df_dataset = pd.read_sql('select * from {Table};',con=con)
    {DatasetCasting}
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    df_dimension_merge = df_dataset.merge(df_dimension, on={MergeOnCol}, how='inner')
    df_agg = df_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    col_list = df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    try:
         for index,row in df_snap.iterrows():
            print(row)
            values = []
            for i in  valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {ReplaceFormat};'''\
            .format(','.join(map(str,values)))
            cur.execute(query)
    except Exception as error:
        print(error)

aggTransformer()


