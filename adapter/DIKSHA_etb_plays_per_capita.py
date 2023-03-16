from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()
print(df_data.columns)
def plays_per_capita():
    df_snap = df_data[['State Code','Plays per capita ( 1st April 2020)']]
    df_snap.columns = ['state_id','plays_per_capita']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'playspercapita-event.data.csv')

plays_per_capita()