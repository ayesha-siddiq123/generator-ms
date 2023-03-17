import configparser
import requests
import os


configuartion_path = os.path.dirname(os.path.abspath(__file__))+"/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

nifi_host = config['CREDs']['nifi_host']
nifi_port = config['CREDs']['nifi_port']
template_files = config['CREDs']['template_files']
header = {"Content-Type": "application/json"}

def get_nifi_root_pg():
    """ Fetch nifi root processor group ID"""
    res = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/root')
    if res.status_code == 200:
        global nifi_root_pg_id
        nifi_root_pg_id = res.json()['component']['id']
        return res.json()['component']['id']
    else:
        return res.text
def get_processor_group_id(processor_group_name):
    nifi_root_pg_id = get_nifi_root_pg()
    pg_list = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
    if pg_list.status_code == 200:
        # Iterate over processGroups and find the required processor group details
        for i in pg_list.json()['processGroupFlow']['flow']['processGroups']:
            if i['component']['name'] == processor_group_name:
                id = i['component']['id']
                return id


def upload_template():
    root_pg_id = get_nifi_root_pg()
    payload = {'template': open(template_files, 'rb')}
    get_template_upload = requests.post(f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/templates/upload", files=payload)
    if get_template_upload.ok:
        print('Successfully uploaded the template',template_files)
    else:
        print("Failed to upload the template ",template_files)

def get_template_id():
    '''Get template id '''
    get_template = requests.get(f"{nifi_host}:{nifi_port}/nifi-api/flow/templates")
    data = get_template.json()
    for template in data['templates']:
        # print(template['template']['name'])
        if template['template']['name'] == 'Plugin to Split CSV':
            template_id = template['template']['id']
            # print(template_id)
            return template_id


def instantiate_template():
    # Instantiates template
    root_pg_id = get_nifi_root_pg()
    template_id = get_template_id()
    data = {
    "templateId": template_id,
    "originX": -1067.5854405025766,
    "originY": -1529.7644241816233,
    "disconnectedNodeAcknowledged": "false"
    }
    get_import_template = requests.post(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/template-instance', json=data)
    if get_import_template.ok:
        print(f"Successfully instantiated the {template_files} in nifi canvas")
    else:
        print(f"Failed to instantiate the {template_files} in nifi canvas ")

def get_controller_services_id(processor_group_name,controllers):
    processor_group_id = get_processor_group_id(processor_group_name)
    list_controllers = requests.get(f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{processor_group_id}/controller-services")
    controllers_list = list_controllers.json()
    for i in controllers_list['controllerServices']:
        if i['component']['name'] == controllers:
            controllers_list_id = i['component']['id']
            return controllers_list_id

def get_controller_services_details(processor_group_name):
    processor_group_id = get_processor_group_id(processor_group_name)
    list_controllers = requests.get(
        f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{processor_group_id}/controller-services")
    controllers_list = list_controllers.json()
    return controllers_list

def enable_controller_services(processor_group_name, controllers):
    controller_id = get_controller_services_id(processor_group_name,controllers)
    controller_services = get_controller_services_details(processor_group_name)
    for i in controller_services['controllerServices']:
        print(i['revision']['version'])
        payload= {"revision": {
                "version": i['revision']['version'],
        }, "state": "ENABLED"}
    enable_service = requests.put(f"{nifi_host}:{nifi_port}/nifi-api/controller-services/{controller_id}/run-status", data=payload, headers=header)
    print(enable_service.text)

def controller_service_enable(processor_group_name):
    controller_details = get_controller_services_details(processor_group_name)
    for i in controller_details['controllerServices']:
        if i['component']['state'] == 'DISABLED':

            controller_service_enable_body = {"revision": {
                "version": i['revision']['version'], }, "state": "ENABLED"}
            controller_service_enable_res = requests.put(f"{nifi_host}:{nifi_port}/nifi-api/controller-services/{i['component']['id']}/run-status",
                                                   json=controller_service_enable_body, headers=header)
            if controller_service_enable_res.status_code == 200:
                print("Successfully enabled the controll services in ", processor_group_name)
            else:
                print("Failed to enabled in ",processor_group_name)

if __name__ == '__main__':
    upload_template()
    instantiate_template()
    controller_service_enable('Plugin to Split CSV')