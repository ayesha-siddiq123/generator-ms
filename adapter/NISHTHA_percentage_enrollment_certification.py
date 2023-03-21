from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_enrolment():
    df_snap = df_data[['State Code','Program','Total Enrolments']]
    df_snap.columns = ['state_id','program_name','total_enrolment']
    obj.upload_file(df_snap, 'totalenrolment-event.data.csv')

def total_completion():
    df_snap = df_data[['State Code','Program','Total Completions']]
    df_snap.columns = ['state_id','program_name','total_completion']
    obj.upload_file(df_snap, 'totalcompletion-event.data.csv')

def total_certificates_issued():
    df_snap = df_data[['State Code', 'Program','Total Certificates Issued']]
    df_snap.columns = ['state_id','program_name','total_certificates_issued']
    obj.upload_file(df_snap, 'totalcertificatesissued-event.data.csv')

def total_courses():
    df_snap = df_data[['State Code','Program', 'Total Courses']]
    df_snap.columns = ['state_id','program_name',  'total_courses']
    obj.upload_file(df_snap, 'totalcourses-event.data.csv')

def doe_event_data():
    df_snap = df_data[['State Code','Program', 'DOE']]
    df_snap.columns = ['state_id','program_name','doe']
    obj.upload_file(df_snap, 'doe-event.data.csv')

def localbody_event_data():
    df_snap = df_data[['State Code', 'Program','Local Body']]
    df_snap.columns = ['state_id','program_name',  'local_body']
    obj.upload_file(df_snap, 'localbody-event.data.csv')

def target_achieved_enrolment():
    df_snap = df_data[['State Code','Program','% Target Achieved- Enrolment']]
    df_snap.columns = ['state_id','program_name','perc_target_achieved_enrolment']
    obj.upload_file(df_snap, 'achievedenrolment-event.data.csv')

def target_achieved_certificates():
    df_snap = df_data[['State Code', 'Program','% Target Achieved- Certificates']]
    df_snap.columns = ['state_id','program_name', 'perc_target_achieved_certificates']
    obj.upload_file(df_snap, 'achievedcertificates-event.data.csv')

def target_remaining_enrolment():
    df_snap = df_data[['State Code', 'Program','% Target Remaining- Enrolment']]
    df_snap.columns = ['state_id', 'program_name','perc_target_remaining_enrolment']
    obj.upload_file(df_snap, 'targetremainingenrolment-event.data.csv')

def target_remaining_certificates():
    df_snap = df_data[['State Code','Program','% Target Remaining- Certificates']]
    df_snap.columns = [ 'state_id','program_name','perc_target_remaining_certificates']
    obj.upload_file(df_snap, 'targetremainingcertificates-event.data.csv')

def total_expected_enrolment():
    df_snap = df_data[['State Code','Program','Total Expected Enrolment']]
    df_snap.columns = ['state_id','program_name','total_expected_enrolment']
    obj.upload_file(df_snap, 'expectedenrolment-event.data.csv')

def total_expected_certification():
    df_snap = df_data[['State Code', 'Program','Total Expected Certification']]
    df_snap.columns = ['state_id','program_name','total_expected_certification']
    obj.upload_file(df_snap, 'expectedcertification-event.data.csv')

if df_data is not None:
    total_enrolment()
    total_completion()
    total_certificates_issued()
    total_courses()
    doe_event_data()
    localbody_event_data()
    target_achieved_enrolment()
    target_achieved_certificates()
    target_remaining_enrolment()
    target_remaining_certificates()
    total_expected_enrolment()
    total_expected_certification()


