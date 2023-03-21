from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()

def total_meal_served():
    df_snap = df_data[['District Code', 'MealServed(02/July/2022)']]
    df_snap.columns = ['district_id', 'total_meals_served']
    obj.upload_file(df_snap, 'totalmealserved-event.data.csv')

def category_event_data():
    df_melt=df_data.melt(id_vars=['District Code'],
                     value_vars=['Enrolled In July','Total Schools'],
                     var_name="category_name",value_name="category_value")
    df_snap=df_melt[['District Code','category_name','category_value']]
    df_snap.columns=['district_id','category_name','category_value']
    obj.upload_file(df_snap, 'category-event.data.csv')
    return df_snap

def category_dimenstion_data():
    df_data=category_event_data()
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    obj.upload_file(df_snap, 'categorypm-dimension.data.csv')

if df_data is not None:
    total_meal_served()
    category_event_data()
    category_dimenstion_data()
