from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()

print(df_data.columns)
def started_event_data():
    df_snap = df_data[['Program','State Code','Started']]
    df_snap.columns = ['program_name','state_id','started']
    df_snap.to_csv(output_path +'/' + program + '/started-event.data.csv', index=False)


started_event_data()

