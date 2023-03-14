from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()
print(df_data.columns)

def qr_coverage():
    df_snap = df_data[['Textbook ID','Textbook Name','Grade','Subject','Medium','Day of Created On','QR Coverage']]
    df_snap.columns = ['textbook_id','textbook_name','grade','subject','medium','day_of_created_on','qr_coverage']
    df_snap.to_csv(output_path + '/' + program + '/qrcoverage-event.data.csv', index=False)

def qr_code_linked_to_content():
    df_snap = df_data[['Textbook ID','Textbook Name','Grade','Subject','Medium','Day of Created On','QR Codes linked to Content']]
    df_snap.columns = ['textbook_id','textbook_name','grade','subject','medium','day_of_created_on','qr_codes_linked_to_content']
    df_snap.to_csv(output_path + '/' + program + '/contentqrcode-event.data.csv', index=False)

def total_qr_codes():
    df_snap = df_data[['Textbook ID', 'Textbook Name', 'Grade', 'Subject', 'Medium', 'Day of Created On','Total QR Codes']]
    df_snap.columns = ['textbook_id', 'textbook_name', 'grade', 'subject', 'medium', 'day_of_created_on','total_qr_codes']
    df_snap.to_csv(output_path + '/' + program + '/totalqrcode-event.data.csv', index=False)


qr_coverage()
qr_code_linked_to_content()
total_qr_codes()


