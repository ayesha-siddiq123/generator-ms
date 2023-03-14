
from main import CollectData

obj=CollectData()
obj.create_dir()
output_path=obj.output_path
program=obj.program
df_data=obj.column_rename()

print(df_data.columns)
def totalenrolment_event_data():
    df_snap = df_data[['Program','State Code','Total Enrolments']]
    df_snap.columns = ['program_name','state_id','total_enrolment']
    df_snap.to_csv(output_path +'/' + program + '/totalenrolment-event.data.csv', index=False)
    print(df_snap.to_string())

def totalcompletion_event_data():
    df_snap = df_data[['Program',  'State Code', 'Total Completions']]
    df_snap.columns = ['program_name',  'state_id', 'total_completion']
    df_snap.to_csv(output_path + '/' + program + '/totalcompletion-event.data.csv', index=False)


def totalcertificatesissued_event_data():
    df_snap = df_data[['Program', 'State Code', 'Total Certificates Issued']]
    df_snap.columns = ['program_name', 'state_id', 'total_certificates_issued']
    df_snap.to_csv(output_path + '/' + program + '/totalcertificatesissued-event.data.csv', index=False)


def totalcourses_event_data():
    df_snap = df_data[['Program','State Code', 'Total Courses']]
    df_snap.columns = ['program_name', 'state_id', 'total_courses']
    df_snap.to_csv(output_path + '/' + program + '/totalcourses-event.data.csv', index=False)

def doe_event_data():
    df_snap = df_data[['Program','State Code', 'DOE']]
    df_snap.columns = ['program_name', 'state_id', 'doe']
    df_snap.to_csv(output_path + '/' + program + '/doe-event.data.csv', index=False)

def localbody_event_data():
    df_snap = df_data[['Program','State Code', 'Local Body']]
    df_snap.columns = ['program_name', 'state_id', 'local_body']
    df_snap.to_csv(output_path + '/' + program + '/localbody-event.data.csv', index=False)

def perctargetachievedenrolment_event_data():
    df_snap = df_data[['Program',  'State Code', '% Target Achieved- Enrolment']]
    df_snap.columns = ['program_name',  'state_id', 'perc_target_achieved_enrolment']
    df_snap.to_csv(output_path + '/' + program + '/perctargetachievedenrolment-event.data.csv', index=False)

def perctargetachievedcertificates_event_data():
    df_snap = df_data[['Program', 'State Code', '% Target Achieved- Certificates']]
    df_snap.columns = ['program_name', 'state_id', 'perc_target_achieved_certificates']
    df_snap.to_csv(output_path +  '/' + program + '/perctargetachievedcertificates-event.data.csv', index=False)

def perctargetremainingenrolment_event_data():
    df_snap = df_data[['Program', 'State Code', '% Target Remaining- Enrolment']]
    df_snap.columns = ['program_name', 'state_id', 'perc_target_remaining_enrolment']
    df_snap.to_csv(output_path + '/' + program + '/perctargetremainingenrolment-event.data.csv', index=False)

def perctargetremainingcertificates_event_data():
    df_snap = df_data[['Program',  'State Code', '% Target Remaining- Certificates']]
    df_snap.columns = ['program_name',  'state_id', 'perc_target_remaining_certificates']
    df_snap.to_csv(output_path +  '/' + program + '/perctargetremainingcertificates-event.data.csv', index=False)

def totalexpectedenrolment_event_data():
    df_snap = df_data[['Program',  'State Code', 'Total Expected Enrolment']]
    df_snap.columns = ['program_name',  'state_id', 'total_expected_enrolment']
    df_snap.to_csv(output_path +  '/' + program + '/totalexpectedenrolment-event.data.csv', index=False)

def totalexpectedcertification_event_data():
    df_snap = df_data[['Program',  'State Code', 'Total Expected Certification']]
    df_snap.columns = ['program_name','state_id', 'total_expected_certification']
    df_snap.to_csv(output_path + '/' + program + '/totalexpectedcertification-event.data.csv', index=False)



totalenrolment_event_data()
totalcompletion_event_data()
totalcertificatesissued_event_data()
totalcourses_event_data()
doe_event_data()
localbody_event_data()
perctargetachievedenrolment_event_data()
perctargetachievedcertificates_event_data()
perctargetremainingenrolment_event_data()
perctargetremainingcertificates_event_data()
totalexpectedenrolment_event_data()
totalexpectedcertification_event_data()

