from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

print(df_data.columns)
def started_event_data():
    df_snap = df_data[['State Code','Program','Started']]
    df_snap.columns = ['state_id','program_name','started']
    obj.upload_file(df_snap, 'programstarted-event.data.csv')


started_event_data()

