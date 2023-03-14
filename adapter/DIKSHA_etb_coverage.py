from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()

def linked_qr_count():
    df_snap = df_data[['TB Id','Grade','Subject','Medium','Linked QR Count']]
    df_snap.columns = ['tb_id','grade','subject','medium','linked_qr_count']
    df_snap.to_csv(output_path + '/' + program + '/linkedqrcount-event.data.csv', index=False)

def resource_count():
    df_snap = df_data[['TB Id','Grade','Subject','Medium','Resource Count']]
    df_snap.columns = ['tb_id','grade','subject','medium','resource_count']
    df_snap.to_csv(output_path + '/' + program + '/resourcecount-event.data.csv', index=False)

linked_qr_count()
resource_count()