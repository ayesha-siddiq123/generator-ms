from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def linked_qr_count():
    df_snap = df_data[['TB Id','Grade','Subject','Medium','Linked QR Count']]
    df_snap.columns = ['tb_id','grade','subject','medium','linked_qr_count']
    obj.upload_file(df_snap,'linkedqrcount-event.data.csv')

def resource_count():
    df_snap = df_data[['TB Id','Grade','Subject','Medium','Resource Count']]
    df_snap.columns = ['tb_id','grade','subject','medium','resource_count']
    obj.upload_file(df_snap, 'resourcecount-event.data.csv')

if df_data is not None:
    linked_qr_count()
    resource_count()