from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()
def totalenrolment_event_data():
    df_snap = df_data[['Program','State Code','District Code','User District_Old', 'Total Enrollments']]
    df_snap.columns = ['program_name','state_id','district_id','user_district_old', 'total_enrolment']
    df_snap.to_csv(output_path + '/' + program + '/totalenrolment-event.data.csv', index=False)

def totalcompletion_event_data():
    df_snap = df_data[['Program','State Code','District Code','User District_Old', 'Total Completion']]
    df_snap.columns = ['program_name', 'state_id','district_id','user_district_old', 'total_completion']
    df_snap.to_csv(output_path + '/' + program + '/totalcompletion-event.data.csv', index=False)

def totalcertification_event_data():
    df_snap = df_data[['Program', 'State Code', 'District Code', 'User District_Old', 'Total Certifications']]
    df_snap.columns = ['program_name', 'state_id', 'district_id', 'user_district_old', 'total_certification']
    df_snap.to_csv(output_path + '/' + program + '/totalcertification-event.data.csv', index=False)


def perccertification_event_data():
    df_snap = df_data[['Program', 'State Code', 'District Code', 'User District_Old', 'Certification %']]
    df_snap.columns = ['program_name', 'state_id', 'district_id', 'user_district_old', 'perc_certification']
    df_snap.to_csv(output_path +'/' + program + '/perccertification-event.data.csv', index=False)


def program_dimension_data():
    df_snap = df_data[['Program']].drop_duplicates()
    df_snap['program_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['program_id', 'Program']]
    df_snap.columns = ['program_id', 'program_name']
    df_snap.to_csv(output_path +  '/' + program + '/program-dimensdion.data.csv', index=False)


totalenrolment_event_data()
totalcompletion_event_data()
totalcertification_event_data()
perccertification_event_data()
program_dimension_data()
