from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()
print(df_data.columns)

def qr_coverage():
    df_snap = df_data[['Textbook ID','Textbook Name','Grade','Subject','Medium','Day of Created On','QR Coverage']]
    df_snap.columns = ['textbook_id','textbook_name','grade','subject','medium','day_of_created_on','qr_coverage']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'qrcoverage-event.data.csv')


def qr_code_linked_to_content():
    df_snap = df_data[['Textbook ID','Textbook Name','Grade','Subject','Medium','Day of Created On','QR Codes linked to Content']]
    df_snap.columns = ['textbook_id','textbook_name','grade','subject','medium','day_of_created_on','qr_codes_linked_to_content']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'contentqrcode-event.data.csv')


def total_qr_codes():
    df_snap = df_data[['Textbook ID', 'Textbook Name', 'Grade', 'Subject', 'Medium', 'Day of Created On','Total QR Codes']]
    df_snap.columns = ['textbook_id', 'textbook_name', 'grade', 'subject', 'medium', 'day_of_created_on','total_qr_codes']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalqrcode-event.data.csv')


qr_coverage()
qr_code_linked_to_content()
total_qr_codes()


