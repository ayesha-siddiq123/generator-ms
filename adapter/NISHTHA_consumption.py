from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()
def totalenrolment_event_data():
    df_snap = df_data[['State Code','District Code','Program', 'Total Enrollments']]
    df_snap.columns = ['state_id','district_id','program_name', 'total_enrolment']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'consumptionenrolment-event.data.csv')


def totalcompletion_event_data():
    df_snap = df_data[['State Code','District Code','Program', 'Total Completion']]
    df_snap.columns = ['state_id','district_id','program_name', 'total_completion']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'consumptioncompletion-event.data.csv')


def totalcertification_event_data():
    df_snap = df_data[['State Code', 'District Code', 'Program', 'Total Certifications']]
    df_snap.columns = ['state_id', 'district_id', 'program_name',  'total_certification']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'consumptioncertification-event.data.csv')


def perccertification_event_data():
    df_snap = df_data[['State Code', 'District Code', 'Program', 'Certification %']]
    df_snap.columns = ['state_id', 'district_id', 'program_name', 'perc_certification']
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
