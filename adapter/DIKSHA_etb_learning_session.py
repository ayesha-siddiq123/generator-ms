from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_plays():
    df_snap = df_data[['State Code','Grade','Subject','Medium','Total No of Plays (App and Portal)']]
    df_snap.columns = ['state_id','grade_diksha','subject_diksha','medium','total_no_of_plays_app_and_portal']
    obj.upload_file(df_snap, 'totalplays-event.data.csv')

def avg_play_time():
    df_snap = df_data[['State Code','Grade','Subject','Medium','Average Play Time in mins (On App and Portal)']]
    df_snap.columns = ['state_id','grade_diksha','subject_diksha','medium','avg_play_time_in_mins_on_app_and_portal']
    obj.upload_file(df_snap, 'avgplaytime-event.data.csv')

if df_data is not None:
    total_plays()
    avg_play_time()
