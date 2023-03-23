from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def school_event_data():
    df_snap = df_data[['State Code','District Code','Number of Schools']]
    df_snap.columns = ['state_id','district_id','no_of_schools']
    obj.upload_file(df_snap, 'schools-event.data.csv')

def teacher_event_data():
    df_snap = df_data[['State Code','District Code','Number of Teachers']]
    df_snap.columns = ['state_id','district_id','no_of_teachers']
    obj.upload_file(df_snap, 'teachers-event.data.csv')

def student_event_data():
    df_snap = df_data[['State Code', 'District Code', 'Students Surveyed']]
    df_snap.columns = ['state_id', 'district_id', 'students_surveyed']
    obj.upload_file(df_snap, 'studentssurveyed-event.data.csv')

def performance_event_data():
    df_snap = df_data[['State Code','District Code','Grade','Subject','Indicator Code','Performance']]
    df_snap.columns = ['state_id','district_id','grade_diksha','subject_diksha','indicator_code','performance']
    obj.upload_file(df_snap, 'performance-event.data.csv')

def learning_outcome_dimension_data():
    df_snap = df_data[['Indicator','Indicator Code']].drop_duplicates()
    df_snap['lo_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['lo_id','Indicator Code','Indicator']]
    df_snap.columns = ['lo_id','lo_code','lo_name']
    obj.upload_file(df_snap, 'lo-dimension.data.csv')



if df_data is not None:
    school_event_data()
    teacher_event_data()
    student_event_data()
    performance_event_data()
    learning_outcome_dimension_data()

