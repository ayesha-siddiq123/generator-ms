from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()

def category_event_data():
    df_data['Overall']=df_data["Grand Total"]
    df_melt=df_data.melt(id_vars=['District code','State Code'],
                     value_vars=["Outcome","Effective Classroom Transaction","Infrastructure, Facilities, Student Entitlements","School Safety and Child Protection","Digital Learning","Governance Processes","Overall"],
                     var_name="category_name",value_name="category_value")
    df_snap=df_melt[['State Code','District code','category_name','category_value']]
    df_snap.columns=['state_id','district_id','category_name','category_value']
    obj.upload_file(df_snap, 'category-event.data.csv')
    return df_snap

def category_dimenstion_data():
    df_data=category_event_data()
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    obj.upload_file(df_snap, 'categorypgi-dimension.data.csv')

if df_data is not None:
    category_event_data()
    category_dimenstion_data()