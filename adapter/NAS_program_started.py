from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()
def program_status():
    df_snap = df_data[['State Code','Started']]
    df_snap.columns = ['state_id','started']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'programstarted-event.data.csv')

program_status()
