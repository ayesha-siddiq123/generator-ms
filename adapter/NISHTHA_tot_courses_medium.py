from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()

print(df_data.columns)
def totalmedium_event_data():
    df_snap = df_data[['Program Name','State Code',' Total Medium']]
    df_snap.columns = ['program_name','state_id','total_medium']
    df_snap.to_csv(output_path +'/' + program + '/totalmedium-event.data.csv', index=False)

def totalcourses_event_data():
    df_snap = df_data[['Program Name','State Code','Total Courses']]
    df_snap.columns = ['program_name','state_id','total_courses']
    df_snap.to_csv(output_path +'/' + program + '/totalcourses-event.data.csv', index=False)


totalmedium_event_data()
totalcourses_event_data()
# state_dimension_data()

