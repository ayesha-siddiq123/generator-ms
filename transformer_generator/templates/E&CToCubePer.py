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
# aws_key = config['CREDs']['AWS_ACCESS_KEY']
# aws_secret=config['CREDs']['AWS_SECRET_ACCESS_KEY']
# bucket_name=config['CREDs']['AWS_BUCKET_NAME']
# object_key=config['CREDs']['AWS_OBJ_KEY']

engine='postgresql://'+user+':%s@'+host+':'+port+'/'+database
con=create_engine(engine %quote(password))
cur = con.connect()

def aggTransformer(valueCols={ValueCols}):
    # path = 's3://{AWSKey}:{AWSSecretKey}@{BucketName}/{ObjKey}'.format(aws_key, aws_secret, bucket_name, object_key)
    # df_events = pd.read_csv(smart_open(path))
    df_events = pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/events/" + {KeyFile})
    {EventCasting}
    df_dataset=pd.read_sql('select * from {Table};',con=con)
    {DatasetCasting}
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    df_dimension_merge = df_events.merge(df_dimension, on={MergeOnCol}, how='inner')
    df_agg = df_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    merge_on_col_list=[]
    for i,j in (df_agg.columns.to_list(),df_dataset.columns.to_list()):
        if(i==j):
            merge_on_col_list.append(j)
    df_merge=df_agg.merge(df_dataset,on=merge_on_col_list,how='inner')
    df_merge_col_list=df_merge.columns.to_list()
    numerator=df_merge_col_list[-2]
    denominator=df_merge_col_list[-1]
    df_merge['percentage'] = ((df_merge[numerator] / df_merge[denominator]) * 100)  ### Calculating Percentage
    col_list = df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    df_snap.columns = valueCols
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {ReplaceFormat};'''\
            .format(','.join(map(str,values)))
            cur.execute(query)
    except Exception as error:
        print(error)

aggTransformer()
