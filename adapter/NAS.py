from main import CollectData
import re

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
state=obj.state
program=obj.program
df_data=obj.column_rename()
def school_event_data():
    df_snap = df_data[['State Code','District Code','Number of Schools']]
    df_snap.columns = ['state_id','district_id','no_of_schools']
    df_snap.to_csv(output_path + '/' + state + '/' + program + '/schools-event.data.csv', index=False)

def teacher_event_data():
    df_snap = df_data[['State Code','District Code','Number of Teachers']]
    df_snap.columns = ['state_id','district_id','no_of_teachers']
    df_snap.to_csv(output_path + '/' + state + '/' + program + '/teachers-event.data.csv', index=False)

def student_event_data():
    df_snap = df_data[['State Code', 'District Code', 'Students Surveyed']]
    df_snap.columns = ['state_id', 'district_id', 'students_surveyed']
    df_snap.to_csv(output_path + '/' + state + '/' + program + '/studentssurveyed-event.data.csv', index=False)

def performance_event_data():
    df_snap = df_data[['State Code','District Code','Subject','Grade','Indicator Code','Performance']]
    df_snap.columns = ['state_id','district_id','subject_name','grade_name','indicator_code','performance']
    df_snap.to_csv(output_path + '/' + state + '/' + program + '/performance-event.data.csv', index=False)

def subject_dimension_data():
    df_snap = df_data[['Subject']].drop_duplicates()
    df_snap['subject_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['subject_id','Subject']]
    df_snap.columns=['subject_id','subject_name']
    df_snap.to_csv(output_path + '/' + state + '/' + program + '/subject-dimension.data.csv', index=False)

def indicator_dimension_data():
    df_snap = df_data[['Indicator','Indicator Code']].drop_duplicates()
    df_snap['indicator_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['indicator_id','Indicator Code','Indicator']]
    df_snap.columns = ['indicator_id','indicator_code','indicator_name']
    df_snap.to_csv(output_path + '/' + state + '/' + program + '/indicator-dimension.data.csv', index=False)

def grade_dimension_data():
    df_snap = df_data[['Grade']].drop_duplicates()
    df_snap['grade_id'] = df_snap['Grade'].apply(lambda x: ''.join(re.findall('\d+', str(x))))
    df_snap = df_snap[['grade_id','Grade']]
    df_snap.columns=['grade_id','grade_name']
    df_snap.to_csv(output_path + '/' + state + '/' + program + '/grade-dimension.data.csv', index=False)

school_event_data()
teacher_event_data()
student_event_data()
performance_event_data()
subject_dimension_data()
indicator_dimension_data()
grade_dimension_data()

