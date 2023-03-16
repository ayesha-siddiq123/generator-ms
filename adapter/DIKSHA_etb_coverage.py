from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()
csv_data=df_data.to_csv(index=False)
obj.upload_file(csv_data,'linkedqrcount-event.data.csv')

def linked_qr_count():
    df_snap = df_data[['TB Id','Grade','Subject','Medium','Linked QR Count']]
    df_snap.columns = ['tb_id','grade','subject','medium','linked_qr_count']
    csv_data=df_snap.to_csv(index=False)
    obj.upload_file(csv_data,'linkedqrcount-event.data.csv')
def resource_count():
    df_snap = df_data[['TB Id','Grade','Subject','Medium','Resource Count']]
    df_snap.columns = ['tb_id','grade','subject','medium','resource_count']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'resourcecount-event.data.csv')

linked_qr_count()
resource_count()