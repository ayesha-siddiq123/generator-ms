from main import CollectData
import re

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()
def total_plays():
    df_snap = df_data[['State Code','Grade','Subject','Medium','Board','Category','Content Name','Mime Type','Total No of Plays (App and Portal)']]
    df_snap.columns = ['state_id','grade','subject','medium','board','category','content_name','mime_type','total_no_of_plays_app_and_portal']
    df_snap.to_csv(output_path + '/' + program + '/totalplays-event.data.csv', index=False)

def avg_play_time():
    df_snap = df_data[['State Code','Grade','Subject','Medium','Board','Category','Content Name','Mime Type','Average Play Time in mins (On App and Portal)']]
    df_snap.columns = ['state_id','grade','subject','medium','board','category','content_name','mime_type','avg_play_time_in_mins_on_app_and_portal']
    df_snap.to_csv(output_path + '/' + program + '/avgplaytime-event.data.csv', index=False)

def subject_dimension_data():
    df_snap = df_data[['Subject']].drop_duplicates()
    df_snap['subject_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['subject_id','Subject']]
    df_snap.columns=['subject_id','subject_name']
    df_snap.to_csv(output_path + '/' + program + '/subject-dimension.data.csv', index=False)

def medium_dimension_data():
    df_snap = df_data[['Medium']].drop_duplicates()
    df_snap['medium_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['medium_id','Medium']]
    df_snap.columns = ['medium_id','medium']
    df_snap.to_csv(output_path + '/' + program + '/medium-dimension.data.csv', index=False)

def grade_dimension_data():
    df_snap = df_data[['Grade']].drop_duplicates()
    df_snap['grade_id'] = df_snap['Grade'].apply(lambda x: ''.join(re.findall('\d+', str(x))))
    df_snap = df_snap[['grade_id','Grade']]
    df_snap.columns=['grade_id','grade_name']
    df_snap.to_csv(output_path + '/' + program + '/grade-dimension.data.csv', index=False)


total_plays()
avg_play_time()
subject_dimension_data()
medium_dimension_data()
grade_dimension_data()