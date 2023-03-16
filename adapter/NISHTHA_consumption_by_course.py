from main import CollectData

obj = CollectData()
program = obj.program
df_data = obj.get_file()

def totalenrolment_event_data():
    df_snap = df_data[['Published By', 'User State', 'Course Name', 'Program', 'Enrollments', 'State Code']]
    df_snap.columns = ['published_by', 'user_state', 'course_name', 'program_name', 'total_enrolment', 'state_id']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'courseenrolment-event.data.csv')


def totalcompletion_event_data():
    df_snap = df_data[['Published By', 'User State', 'Course Name', 'Program', 'Completion', 'State Code']]
    df_snap.columns = ['published_by', 'user_state', 'course_name', 'program_name', 'total_completion', 'state_id']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'coursecompletion-event.data.csv')


def totalcertification_event_data():
    df_snap = df_data[['Published By', 'User State', 'Course Name', 'Program', 'Certification', 'State Code']]
    df_snap.columns = ['published_by', 'user_state', 'course_name', 'program_name', 'total_certification', 'state_id']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'coursecertification-event.data.csv')


totalenrolment_event_data()
totalcompletion_event_data()
totalcertification_event_data()

