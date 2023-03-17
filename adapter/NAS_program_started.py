from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def program_status():
    df_snap = df_data[['State Code','Started']]
    df_snap.columns = ['state_id','started']
    obj.upload_file(df_snap, 'programstarted-event.data.csv')

def state_dimension():
    df_snap=df_data[['State Code','State','Latitude','Longitude']]
    df_snap.columns=['state_id','state_name','latitude','longitude']
    obj.upload_file(df_snap,'state-dimension.data.csv')


program_status()
state_dimension()
