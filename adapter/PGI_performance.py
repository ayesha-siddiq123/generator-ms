from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()

def category_event_data():
    df_data['Overall']=df_data["Grand Total"]
    df_melt=df_data.melt(id_vars=['State Code'],
                     value_vars=["Learning Outcomes & Quality","Access","Infrastructure & Facilities","Equity","Governance Processes"],
                     var_name="category_name",value_name="category_value")
    df_snap=df_melt[['State Code','category_name','category_value']]
    df_snap.columns=['state_id','category_name','category_value']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'performancecategory-event.data.csv')
    return df_snap


def category_dimenstion_data():
    df_data=category_event_data()
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'performancecategory-dimension.data.csv')


category_event_data()
category_dimenstion_data()