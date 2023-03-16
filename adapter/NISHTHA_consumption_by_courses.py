from main import CollectData

obj = CollectData()
program = obj.program
df_data = obj.get_file()

def totalenrolment_event_data():
    df_snap = df_data[['State Code', 'Program','Course Name', 'Enrollments']]
    df_snap.columns = ['state_id','program_name','course_name','total_enrolment']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'courseenrolment-event.data.csv')


def totalcompletion_event_data():
    df_snap = df_data[['State Code','Program','Course Name','Completion']]
    df_snap.columns = [ 'state_id','program_name','course_name','total_completion']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'coursecompletion-event.data.csv')


def totalcertification_event_data():
    df_snap = df_data[['State Code','Program','Course Name', 'Certification']]
    df_snap.columns = ['state_id','program_name','course_name','total_certification']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'coursecertification-event.data.csv')


totalenrolment_event_data()
totalcompletion_event_data()
totalcertification_event_data()

