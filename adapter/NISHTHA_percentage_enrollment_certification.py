
from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

print(df_data.columns)
def totalenrolment_event_data():
    df_snap = df_data[['State Code','Program','Total Enrolments']]
    df_snap.columns = ['state_id','program_name','total_enrolment']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalenrolment-event.data.csv')


def totalcompletion_event_data():
    df_snap = df_data[['State Code','Program','Total Completions']]
    df_snap.columns = ['state_id','program_name','total_completion']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalcompletion-event.data.csv')


def totalcertificatesissued_event_data():
    df_snap = df_data[['State Code', 'Program','Total Certificates Issued']]
    df_snap.columns = ['state_id','program_name','total_certificates_issued']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalcertificatesissued-event.data.csv')


def totalcourses_event_data():
    df_snap = df_data[['State Code','Program', 'Total Courses']]
    df_snap.columns = ['state_id','program_name',  'total_courses']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalcourses-event.data.csv')


def doe_event_data():
    df_snap = df_data[['State Code','Program', 'DOE']]
    df_snap.columns = ['state_id','program_name','doe']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'doe-event.data.csv')


def localbody_event_data():
    df_snap = df_data[['State Code', 'Program','Local Body']]
    df_snap.columns = ['state_id','program_name',  'local_body']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'localbody-event.data.csv')


def perctargetachievedenrolment_event_data():
    df_snap = df_data[['State Code','Program','% Target Achieved- Enrolment']]
    df_snap.columns = ['state_id','program_name','perc_target_achieved_enrolment']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'perctargetachievedenrolment-event.data.csv')


def perctargetachievedcertificates_event_data():
    df_snap = df_data[['State Code', 'Program','% Target Achieved- Certificates']]
    df_snap.columns = ['state_id','program_name', 'perc_target_achieved_certificates']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'perctargetachievedcertificates-event.data.csv')


def perctargetremainingenrolment_event_data():
    df_snap = df_data[['State Code', 'Program','% Target Remaining- Enrolment']]
    df_snap.columns = ['state_id', 'program_name','perc_target_remaining_enrolment']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'perctargetremainingenrolment-event.data.csv')


def perctargetremainingcertificates_event_data():
    df_snap = df_data[['State Code','Program','% Target Remaining- Certificates']]
    df_snap.columns = [ 'state_id','program_name','perc_target_remaining_certificates']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'perctargetremainingcertificates-event.data.csv')


def totalexpectedenrolment_event_data():
    df_snap = df_data[['State Code','Program','Total Expected Enrolment']]
    df_snap.columns = ['state_id','program_name','total_expected_enrolment']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalexpectedenrolment-event.data.csv')


def totalexpectedcertification_event_data():
    df_snap = df_data[['State Code', 'Program','Total Expected Certification']]
    df_snap.columns = ['state_id','program_name','total_expected_certification']
    csv_data = df_snap.to_csv(index=False)
    obj.upload_file(csv_data, 'totalexpectedcertification-event.data.csv')


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

