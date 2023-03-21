from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def program_status():
    df_snap = df_data[['State Code','Started']]
    df_snap.columns=['state_id','started']
    obj.upload_file(df_snap, 'udiseprogramstarted-event.data.csv')

if df_data is not None:
    program_status()