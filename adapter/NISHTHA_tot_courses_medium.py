from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.column_rename()

def totalmedium_event_data():
    df_snap = df_data[['State Code','Program Name','Total Medium']]
    df_snap.columns = ['state_id','program_name','total_medium']
    obj.upload_file(df_snap, 'totalmedium-event.data.csv')

def totalcourses_event_data():
    df_snap = df_data[['State Code','Program Name','Total Courses']]
    df_snap.columns = ['state_id','program_name','total_courses']
    obj.upload_file(df_snap, 'mediumtotalcourses-event.data.csv')

if df_data is not None:
    totalmedium_event_data()
    totalcourses_event_data()


