from main import CollectData
import re

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()
def program_status():
    df_snap = df_data[['State Code','Started']]
    df_snap.columns = ['state_id','started']
    df_snap.to_csv(output_path + '/' + program + '/programstarted-event.data.csv', index=False)

program_status()
