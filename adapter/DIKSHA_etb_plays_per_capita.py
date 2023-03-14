from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()
print(df_data.columns)
def plays_per_capita():
    df_snap = df_data[['State Code','Plays per capita ( 1st April 2020)']]
    df_snap.columns = ['state_id','plays_per_capita']
    df_snap.to_csv(output_path + '/' + program + '/playspercapita-event.data.csv', index=False)

plays_per_capita()