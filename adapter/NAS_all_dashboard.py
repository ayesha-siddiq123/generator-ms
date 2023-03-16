from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def school_event_data():
    df_snap = df_data[['State Code','District Code','Number of Schools']]
    df_snap.columns = ['state_id','district_id','no_of_schools']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'schools-event.data.csv')

def teacher_event_data():
    df_snap = df_data[['State Code','District Code','Number of Teachers']]
    df_snap.columns = ['state_id','district_id','no_of_teachers']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'teachers-event.data.csv')

def student_event_data():
    df_snap = df_data[['State Code', 'District Code', 'Students Surveyed']]
    df_snap.columns = ['state_id', 'district_id', 'students_surveyed']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'studentssurveyed-event.data.csv')

def performance_event_data():
    df_snap = df_data[['State Code','District Code','Subject','Grade','Indicator Code','Performance']]
    df_snap.columns = ['state_id','district_id','subject_name','grade_name','indicator_code','performance']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'performance-event.data.csv')

def subject_dimension_data():
    df_snap = df_data[['Subject']].drop_duplicates()
    df_snap['subject_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['subject_id','Subject']]
    df_snap.columns=['subject_id','subject_name']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'subject-dimension.data.csv')

def learning_outcome_dimension_data():
    df_snap = df_data[['Indicator','Indicator Code']].drop_duplicates()
    df_snap['lo_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['lo_id','Indicator Code','Indicator']]
    df_snap.columns = ['lo_id','lo_code','lo_name']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'lo-dimension.data.csv')

def grade_dimension_data():
    df_snap = df_data[['Grade']].drop_duplicates()
    df_snap['grade_id'] = df_snap['Grade'].apply(lambda x: ''.join(re.findall('\d+', str(x))))
    df_snap = df_snap[['grade_id','Grade']]
    df_snap.columns=['grade_id','grade_name']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'grade-dimension.data.csv')


school_event_data()
teacher_event_data()
student_event_data()
performance_event_data()
subject_dimension_data()
learning_outcome_dimension_data()
grade_dimension_data()

