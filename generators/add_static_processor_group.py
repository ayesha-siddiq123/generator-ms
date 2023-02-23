import random
import requests as rq
import os
import configparser
from update_json_property import update_processor_property

configuartion_path = os.path.dirname(os.path.abspath(__file__))+"/transformers/python_files/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

server_url = config['CREDs']['server_url']
nifi_host = config['CREDs']['nifi_host']
nifi_port = config['CREDs']['nifi_port']
def get_nifi_root_pg():
    """ Fetch nifi root processor group ID"""
    res = rq.get(f':{nifi_port}/nifi-api/process-groups/root')
    if res.status_code == 200:
        global nifi_root_pg_id
        nifi_root_pg_id = res.json()['component']['id']
        return res.json()['component']['id']
    else:
        return res.text

def add_processor_group(pg_name):
    nifi_root_pg_id = get_nifi_root_pg()
    min_range = -2000
    max_range = 5000
    x = random.randint(min_range, max_range)
    y = random.randrange(min_range, max_range)
    pg_details = {
        "revision": {
            "clientId": "python_code:add_processor.py",
            "version": 0
        },
        "disconnectedNodeAcknowledged": "false",
        "component": {
            "name": pg_name,
            "position": {
                "x": x,
                "y": y
            }
        }
    }
    res = rq.post(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/{nifi_root_pg_id}/process-groups', json=pg_details)
    if res.ok:
        print("Successfully created the processor group", pg_name)
    else:
        print("Failed to create the processor group", pg_name)

def get_processor_group_info(processor_group_name):
    """
    Get procesor group details
    """
    nifi_root_pg_id = get_nifi_root_pg()
    pg_list = rq.get(f'{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
    if pg_list.status_code == 200:
        # Iterate over processGroups and find the required processor group details
        for i in pg_list.json()['processGroupFlow']['flow']['processGroups']:
            if i['component']['name'] == processor_group_name:
                global processor_group
                processor_group = i
                return i
    else:
        return 'failed to list the processor groups'

def get_processor_group_ports(processor_group_name):
    # Get processor group details
    global pg_source
    pg_source = get_processor_group_info(processor_group_name)
    pg_details = rq.get(f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{pg_source['component']['id']}")
    if pg_details.status_code != 200:
        return pg_details.text
    else:
        return pg_details

def get_processor_id(processor_group_name, processor_name):
    # Get the processors in the processor group
    pg_source = get_processor_group_ports(processor_group_name)
    if pg_source.status_code == 200:
        for i in pg_source.json()['processGroupFlow']['flow']['processors']:
            # Get the required processor details
            if i['component']['name'] == processor_name:
                id = i['component']['id']
                return id

def get_output_port_id(processor_group_name, name):
    # Get the processors in the processor group
    pg_source = get_processor_group_ports(processor_group_name)
    if pg_source.status_code == 200:
        for i in pg_source.json()['processGroupFlow']['flow']['outputPorts']:
            if i['component']['name'] == name:
                output_port_id = i['component']['id']
                return output_port_id

def get_input_port_id(processor_group_name, name):
    # Get the processors in the processor group
    pg_source = get_processor_group_ports(processor_group_name)
    if pg_source.status_code == 200:
        for i in pg_source.json()['processGroupFlow']['flow']['inputPorts']:
            if i['component']['name'] == name:
                input_port_id = i['component']['id']
                return input_port_id

def get_processor_group_id(processor_group_name):
    nifi_root_pg_id = get_nifi_root_pg()
    pg_list = rq.get(f'{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
    if pg_list.status_code == 200:
        # Iterate over processGroups and find the required processor group details
        for i in pg_list.json()['processGroupFlow']['flow']['processGroups']:
            if i['component']['name'] == processor_group_name:
                id = i['component']['id']
                return id

def add_processor(processor_group_name, processor_name, name):
    pg_source = get_processor_group_ports(processor_group_name)
    min_range = -250
    max_range = +250
    x = random.randint(min_range, max_range)
    y = random.randrange(min_range, max_range)
    pg_source_json = pg_source.json()
    if name == 'PutS3Object':
        processors = {
            "revision": {
                "clientId": "",
                "version": 0
            },
            "disconnectedNodeAcknowledged": "false",
            "component": {
                "type": f"{processor_name}",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-aws-nar",
                    "version": "1.12.1"
                },
                "name": name,
                "position": {
                    "x": x,
                    "y": y
                }
            }
        }
    else:
        processors = {
            "revision": {
                "clientId": "",
                "version": 0
            },
            "disconnectedNodeAcknowledged": "false",
            "component": {
                "type": f"{processor_name}",
                "bundle": {
                    "group": "org.apache.nifi",
                    "artifact": "nifi-standard-nar",
                    "version": "1.12.1"
                },
                "name": name,
                "position": {
                    "x": x,
                    "y": y
                }
            }
        }


    processor_res = rq.post(
        f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{pg_source_json['processGroupFlow']['id']}/processors",
        json=processors)
    if processor_res.status_code == 201:
        print(f"Successfully created the processor", name)
        return True
    else:
        return processor_res.text

def connect(processor_group_name,sourceid, destinationid, relationship):
    pg_source = get_processor_group_ports(processor_group_name)
    pg_source_json = pg_source.json()
    pg_id = pg_source_json['processGroupFlow']['id']
    json_body = {
        "revision": {
            "clientId": "python code: add_processor.py",
            "version": 0
        },
        "disconnectedNodeAcknowledged": "false",
        "component": {
            "name": "",
            "source": {
                "id": sourceid,
                "groupId": pg_id,
                "type": "PROCESSOR"
            },
            "destination": {
                "id": destinationid,
                "groupId": pg_id,
                "type": "PROCESSOR"
            },
            "selectedRelationships": relationship
        }
    }
    connection = rq.post(
        f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{pg_source_json['processGroupFlow']['id']}/connections",
        json=json_body)
    if connection.ok:
        print(f"Successfully connected the processor from {sourceid} to {destinationid}")
    else:
        print("Failed to connect the processor", connection.text)

def add_ports(processor_group_name, name):
    pg_source = get_processor_group_ports(processor_group_name)
    min_range = 75
    max_range = 2000
    x = random.randint(min_range, max_range)
    y = random.randrange(min_range, max_range)
    pg_source_json = pg_source.json()
    pg_id = pg_source_json['processGroupFlow']['id']
    processors = {
            "revision": {
                "clientId": "",
                "version": 0
            },
            "disconnectedNodeAcknowledged": "false",
            "component": {
                "name": name,
                "allowRemoteAccess": "false",
                "position": {
                    "x": x,
                    "y": y
                }
            }
        }
    if name.__contains__('output'):
        processor_res = rq.post(f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{pg_id}/output-ports",
            json=processors)
        if processor_res.status_code == 201:
            print(f"Successfully created the output ports", name)
            return True
        else:
            return processor_res.text
    if name.__contains__('input'):
        processor_res = rq.post(f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{pg_id}/input-ports",
            json=processors)
        if processor_res.status_code == 201:
            print(f"Successfully created the input ports", name)
            return True
        else:
            return processor_res.text
def connect_ports_inside_pg(processor_group_name,sourceid, source_groupid, source_type, destinationid, destination_groupid,destination_type,relationship):
    nifi_root_id = get_nifi_root_pg()
    processor_group_id = get_processor_group_id(processor_group_name)
    json_body = {
            "revision": {
                "clientId": "",
                "version": 0
            },
            "disconnectedNodeAcknowledged": "false",
            "component": {
                "name": "",
                "source": {
                    "id": sourceid,
                    "groupId": source_groupid,
                    "type": source_type
                },
                "destination": {
                    "id": destinationid,
                    "groupId": destination_groupid,
                    "type": destination_type
                },
                "selectedRelationships": relationship
            }
            }
    if destination_type == 'FUNNEL':
        connection = rq.post(
            f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{nifi_root_id}/connections",
            json=json_body)
        if connection.ok:
            print(f"Successfully connected from {sourceid} to {destinationid}")
        else:
            print("Failed to connect", connection.text)
    else:
        connection = rq.post(
            f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{processor_group_id}/connections",
            json=json_body)
        if connection.ok:
            print(f"Successfully connected from {sourceid} to {destinationid}")
        else:
            print("Failed to connect", connection.text)

def create_funnel():
    get_root_id = get_nifi_root_pg()
    payload = {
            "revision": {
                "clientId": "",
                "version": 0
            },
            "disconnectedNodeAcknowledged": "false",
            "component": {
                "position": {
                    "x": 1558.3453868101176,
                    "y": -1451.0643159390884
                }
            }
        }
    connection = rq.post(
        f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{get_root_id}/funnels",
        json=payload)
    if connection.ok:
        print("Sucessfully created the funnel")
    else:
        print("failed to create the funnel")

def call_file_moving_pg(processor_group_name):
    add_processor_group(processor_group_name)
    add_ports(processor_group_name, 'recieve_event_name_input')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.FetchFile', 'checking_in_processing')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.PutFile', 'putfile_processing')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.LogMessage', 'processed_file_error')
    add_ports(processor_group_name, 'send_moved_file_output')
    recieve_event_name_input_id = get_input_port_id(processor_group_name, 'recieve_event_name_input')
    send_moved_file_output_id = get_output_port_id(processor_group_name, 'send_moved_file_output')
    checking_in_processing_id = get_processor_id(processor_group_name, 'checking_in_processing')
    putfile_processing_id = get_processor_id(processor_group_name, 'putfile_processing')
    processed_file_error_id = get_processor_id(processor_group_name, 'processed_file_error')
    file_move_pg_id = get_processor_group_id(processor_group_name)
    update_processor_property(processor_group_name, 'checking_in_processing')
    update_processor_property(processor_group_name, 'putfile_processing')
    update_processor_property(processor_group_name, 'processed_file_error')
    connect_ports_inside_pg(processor_group_name, recieve_event_name_input_id, file_move_pg_id, 'INPUT_PORT',
                            checking_in_processing_id, file_move_pg_id, 'PROCESSOR', [])
    connect_ports_inside_pg(processor_group_name, checking_in_processing_id, file_move_pg_id, 'PROCESSOR',
                            send_moved_file_output_id, file_move_pg_id, 'OUTPUT_PORT', ['success'])
    connect(processor_group_name, checking_in_processing_id, processed_file_error_id, ["permission.denied"])
    connect(processor_group_name, checking_in_processing_id, putfile_processing_id, ["failure", "not.found"])
    connect(processor_group_name, putfile_processing_id, processed_file_error_id, ["failure"])
    connect_ports_inside_pg(processor_group_name, putfile_processing_id, file_move_pg_id, 'PROCESSOR', send_moved_file_output_id,
                            file_move_pg_id, 'OUTPUT_PORT', ['success'])


def call_update_api_status_pg(processor_group_name):
    processor_name = ['api_badgateway_error','call_api','error_api_call','ExtractText','route_status','failed_toroute_status','processed_to_archived','routing_error_status','success_routing_status']
    add_processor_group(processor_group_name)
    add_ports(processor_group_name, 'receive_json_api_input')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.InvokeHTTP', 'call_api')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.LogMessage', 'api_badgateway_error')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.LogMessage', 'error_api_call')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.ExtractText', 'ExtractText')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.RouteOnAttribute', 'route_status')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.ExecuteStreamCommand', 'processed_to_archived')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.LogMessage', 'routing_error_status')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.LogMessage', 'success_routing_status')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.LogMessage', 'failed_toroute_status')
    update_api_status_id = get_processor_group_id(processor_group_name)
    call_api_id = get_processor_id(processor_group_name,'call_api')
    api_badgateway_error_id = get_processor_id(processor_group_name,'api_badgateway_error')
    error_api_call_id = get_processor_id(processor_group_name, 'error_api_call')
    ExtractText_id = get_processor_id(processor_group_name, 'ExtractText')
    route_status_id = get_processor_id(processor_group_name, 'route_status')
    processed_to_archived_id = get_processor_id(processor_group_name, 'processed_to_archived')
    routing_error_status_id = get_processor_id(processor_group_name, 'routing_error_status')
    success_routing_status_id = get_processor_id(processor_group_name, 'success_routing_status')
    receive_json_api_input_id = get_input_port_id(processor_group_name, 'receive_json_api_input')
    failed_toroute_status_id = get_processor_id(processor_group_name, 'failed_toroute_status')
    for i in processor_name:
        update_processor_property(processor_group_name,i)
    connect_ports_inside_pg(processor_group_name,receive_json_api_input_id,update_api_status_id,'INPUT_PORT',call_api_id,update_api_status_id,'PROCESSOR',[])
    connect(processor_group_name,call_api_id,api_badgateway_error_id,['Retry'])
    connect(processor_group_name,call_api_id,error_api_call_id,["Failure","No Retry"])
    connect(processor_group_name,call_api_id,ExtractText_id,["Response"])
    connect(processor_group_name,ExtractText_id,error_api_call_id,["unmatched"])
    connect(processor_group_name,ExtractText_id, route_status_id, ["matched"])
    connect(processor_group_name,route_status_id,failed_toroute_status_id,["unmatched"])
    connect(processor_group_name,route_status_id,processed_to_archived_id,["file_status"])
    connect(processor_group_name,processed_to_archived_id,routing_error_status_id,["nonzero status"])
    connect(processor_group_name,processed_to_archived_id,success_routing_status_id,["output stream"])

def s3_configuration(processor_group_name):
    add_processor_group(processor_group_name)
    add_processor(processor_group_name,'org.apache.nifi.processors.standard.GetFile', 'GetFile')
    add_processor(processor_group_name,'org.apache.nifi.processors.aws.s3.PutS3Object', 'PutS3Object')
    add_processor(processor_group_name,'org.apache.nifi.processors.standard.LogMessage','success_log_message')
    add_processor(processor_group_name, 'org.apache.nifi.processors.standard.LogMessage', 'failed_log_message')
    getfile_id = get_processor_id(processor_group_name,'GetFile')
    PutS3Object_id = get_processor_id(processor_group_name,'PutS3Object')
    success_log_message_id = get_processor_id(processor_group_name,'success_log_message')
    failed_log_message_id = get_processor_id(processor_group_name,'failed_log_message')
    update_processor_property(processor_group_name,'GetFile')
    update_processor_property(processor_group_name, 'PutS3Object')
    update_processor_property(processor_group_name, 'success_log_message')
    update_processor_property(processor_group_name, 'failed_log_message')
    connect(processor_group_name,getfile_id,PutS3Object_id,['success'])
    connect(processor_group_name,PutS3Object_id,success_log_message_id,['success'])
    connect(processor_group_name,PutS3Object_id,failed_log_message_id,['failure'])

if __name__ == '__main__':
    call_file_moving_pg('File_moving')
    call_update_api_status_pg('update_api_status')
    s3_configuration('UploadToArchiveS3')
    s3_configuration('UploadToErrorS3')


