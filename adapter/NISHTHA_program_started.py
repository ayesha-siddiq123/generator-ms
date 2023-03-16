from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

print(df_data.columns)
def started_event_data():
    df_snap = df_data[['Program','State Code','Started']]
    df_snap.columns = ['program_name','state_id','started']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'programstarted-event.data.csv')


started_event_data()

