from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()
def totalenrolment_event_data():
    df_snap = df_data[['Program','State Code','District Code','User District_Old', 'Total Enrollments']]
    df_snap.columns = ['program_name','state_id','district_id','user_district_old', 'total_enrolment']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'consumptionenrolment-event.data.csv')


def totalcompletion_event_data():
    df_snap = df_data[['Program','State Code','District Code','User District_Old', 'Total Completion']]
    df_snap.columns = ['program_name', 'state_id','district_id','user_district_old', 'total_completion']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'consumptioncompletion-event.data.csv')


def totalcertification_event_data():
    df_snap = df_data[['Program', 'State Code', 'District Code', 'User District_Old', 'Total Certifications']]
    df_snap.columns = ['program_name', 'state_id', 'district_id', 'user_district_old', 'total_certification']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'consumptioncertification-event.data.csv')


def perccertification_event_data():
    df_snap = df_data[['Program', 'State Code', 'District Code', 'User District_Old', 'Certification %']]
    df_snap.columns = ['program_name', 'state_id', 'district_id', 'user_district_old', 'perc_certification']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'consumptionperccertification-event.data.csv')


def program_dimension_data():
    df_snap = df_data[['Program']].drop_duplicates()
    df_snap['program_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['program_id', 'Program']]
    df_snap.columns = ['program_id', 'program_name']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'program-dimension.data.csv')


totalenrolment_event_data()
totalcompletion_event_data()
totalcertification_event_data()
perccertification_event_data()
program_dimension_data()
