from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()
def total_plays():
    df_snap = df_data[['State Code','Grade','Subject','Medium','Board','Category','Content Name','Mime Type','Total No of Plays (App and Portal)']]
    df_snap.columns = ['state_id','grade','subject','medium','board','category','content_name','mime_type','total_no_of_plays_app_and_portal']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalplays-event.data.csv')

def avg_play_time():
    df_snap = df_data[['State Code','Grade','Subject','Medium','Board','Category','Content Name','Mime Type','Average Play Time in mins (On App and Portal)']]
    df_snap.columns = ['state_id','grade','subject','medium','board','category','content_name','mime_type','avg_play_time_in_mins_on_app_and_portal']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'avgplaytime-event.data.csv')
def subject_dimension_data():
    df_snap = df_data[['Subject']].drop_duplicates()
    df_snap['subject_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['subject_id','Subject']]
    df_snap.columns=['subject_id','subject_name']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'subject-dimension.data.csv')

def medium_dimension_data():
    df_snap = df_data[['Medium']].drop_duplicates()
    df_snap['medium_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['medium_id','Medium']]
    df_snap.columns = ['medium_id','medium']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'medium-dimension.data.csv')

def grade_dimension_data():
    df_snap = df_data[['Grade']].drop_duplicates()
    df_snap['grade_id'] = df_snap['Grade'].apply(lambda x: ''.join(re.findall('\d+', str(x))))
    df_snap = df_snap[['grade_id','Grade']]
    df_snap.columns=['grade_id','grade_name']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'grade-dimension.data.csv')


total_plays()
avg_play_time()
subject_dimension_data()
medium_dimension_data()
grade_dimension_data()