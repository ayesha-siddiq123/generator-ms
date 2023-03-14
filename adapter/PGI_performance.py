from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data = obj.column_rename()

def category_event_data():
    df_data['Overall']=df_data["Grand Total"]
    df_melt=df_data.melt(id_vars=['State Code'],
                     value_vars=["Learning Outcomes & Quality","Access","Infrastructure & Facilities","Equity","Governance Processes"],
                     var_name="category_name",value_name="category_value")
    df_snap=df_melt[['State Code','category_name','category_value']]
    df_snap.columns=['state_id','category_name','category_value']
    df_snap.to_csv(output_path + '/' + program +'/performancecategory-event.data.csv',index=False)
    return df_snap


def category_dimenstion_data():
    df_data=category_event_data()
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    df_snap.to_csv(output_path + '/' + program + '/performancecategory-dimension.data.csv',index=False)

category_event_data()
category_dimenstion_data()