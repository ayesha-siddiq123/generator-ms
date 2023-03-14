import pandas

from main import CollectData

obj = CollectData()
obj.create_dir()
output_path = obj.output_path
program = obj.program
df_data = obj.column_rename()


def totalenrolment_event_data():
    df_snap = df_data[['Published By', 'User State', 'Course Name', 'Program', 'Enrollments', 'State Code']]
    df_snap.columns = ['published_by', 'user_state', 'course_name', 'program_name', 'total_enrolment', 'state_id']
    df_snap.to_csv(output_path +  '/' + program + '/totalenrolment-event.data.csv', index=False)


def totalcompletion_event_data():
    df_snap = df_data[['Published By', 'User State', 'Course Name', 'Program', 'Completion', 'State Code']]
    df_snap.columns = ['published_by', 'user_state', 'course_name', 'program_name', 'total_completion', 'state_id']
    df_snap.to_csv(output_path +  '/' + program + '/totalcompletion-event.data.csv', index=False)


def totalcertification_event_data():
    df_snap = df_data[['Published By', 'User State', 'Course Name', 'Program', 'Certification', 'State Code']]
    df_snap.columns = ['published_by', 'user_state', 'course_name', 'program_name', 'total_certification', 'state_id']
    df_snap.to_csv(output_path +  '/' + program + '/totalcertification-event.data.csv', index=False)


totalenrolment_event_data()
totalcompletion_event_data()
totalcertification_event_data()

