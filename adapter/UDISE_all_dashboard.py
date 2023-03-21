from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def students_event_data():
    df_snap = df_data[['State Code','District Code','Number of Students']]
    df_snap.columns=['state_id','district_id','no_of_students']
    obj.upload_file(df_snap, 'students-event.data.csv')

def category_event_data():
    df_melt = df_data.melt(id_vars=['District Code', 'State Code'],
                           value_vars=["PTR","% schools having toilet","% schools having drinking water","% schools having electricity","% schools having library","% govt aided schools received textbook","% schools with Ramp"],
                           var_name="category_name", value_name="category_value")
    df_snap = df_melt[['State Code', 'District Code', 'category_name', 'category_value']]
    df_snap.columns = ['state_id', 'district_id', 'category_name', 'category_value']
    obj.upload_file(df_snap, 'category-event.data.csv')
    return df_snap

def category_dimenstion_data():
    df_data=category_event_data()
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    obj.upload_file(df_snap, 'categoryudise-dimension.data.csv')

if df_data is not None:
    students_event_data()
    category_event_data()
    category_dimenstion_data()
