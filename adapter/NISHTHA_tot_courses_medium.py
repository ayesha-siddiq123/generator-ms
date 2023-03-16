from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.column_rename()

print(df_data.columns)
def totalmedium_event_data():
    df_snap = df_data[['Program Name','State Code',' Total Medium']]
    df_snap.columns = ['program_name','state_id','total_medium']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalmedium-event.data.csv')


def totalcourses_event_data():
    df_snap = df_data[['Program Name','State Code','Total Courses']]
    df_snap.columns = ['program_name','state_id','total_courses']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalcourses-event.data.csv')


totalmedium_event_data()
totalcourses_event_data()


