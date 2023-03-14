from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data = obj.column_rename()
def total_meal_served():
    df_snap = df_data[['District Code', 'MealServed(02/July/2022)']]
    df_snap.columns = ['district_id', 'total_meals_served']
    df_snap.to_csv(output_path + '/' + program + '/totalmealserved-event.data.csv', index=False)

def category_event_data():
    df_melt=df_data.melt(id_vars=['District Code'],
                     value_vars=['Enrolled In July','Total Schools'],
                     var_name="category_name",value_name="category_value")
    df_snap=df_melt[['District Code','category_name','category_value']]
    df_snap.columns=['district_id','category_name','category_value']
    df_snap.to_csv(output_path + '/' + program +'/category-event.data.csv',index=False)
    return df_snap

def category_dimenstion_data():
    df_data=category_event_data()
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    df_snap.to_csv(output_path + '/' + program + '/category-dimension.data.csv',index=False)

total_meal_served()
category_event_data()
category_dimenstion_data()
