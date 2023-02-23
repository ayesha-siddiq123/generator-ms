import requests as rq
import os
import configparser

configuartion_path = os.path.dirname(os.path.abspath(__file__))+"/transformers/python_files/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

url = config['CREDs']['server_url']
error_bucket_name = config['CREDs']['error_bucket_name']
archived_bucket_name = config['CREDs']['archived_bucket_name']
s3_access_key = config['CREDs']['s3_access_key']
s3_secret_key = config['CREDs']['s3_secret_key']
nifi_host = config['CREDs']['nifi_host']
nifi_port = config['CREDs']['nifi_port']

def get_nifi_root_pg():
    """ Fetch nifi root processor group ID"""
    res = rq.get(f'http://{nifi_host}:{nifi_port}/nifi-api/process-groups/root')
    if res.status_code == 200:
        global nifi_root_pg_id
        nifi_root_pg_id = res.json()['component']['id']
        return res.json()['component']['id']
    else:
        return res.text

def get_processor_group_info(processor_group_name):
    """
    Get procesor group details
    """
    nifi_root_pg_id = get_nifi_root_pg()
    pg_list = rq.get(f'http://{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
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
    pg_details = rq.get(f"http://{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{pg_source['component']['id']}")
    if pg_details.status_code != 200:
        return pg_details.text
    else:
        return pg_details


def update_processor_property(processor_group_name, processor_name):
    # Get the processors in the processor group
    pg_source = get_processor_group_ports(processor_group_name)
    if pg_source.status_code == 200:
        for i in pg_source.json()['processGroupFlow']['flow']['processors']:
            # Get the required processor details
            if i['component']['name'] == processor_name:
                # Request body creation to update processor property.
                global update_processor_property_body
                if processor_group_name == 'UploadToArchiveS3':
                    if processor_name == 'GetFile':
                        update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "concurrentlySchedulableTaskCount": "1",
                                    "schedulingPeriod": "* * * * * ?",
                                    "schedulingStrategy": "CRON_DRIVEN",
                                    "properties": {
                                        "Input Directory": "/archived_data"
                                    },
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                    if processor_name == 'PutS3Object':
                        update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                     "properties": {
                                        "Bucket": archived_bucket_name,
                                        "Access Key": s3_access_key,
                                        "Secret Key": s3_secret_key,
                                         "Region": "ap-south-1"
                                     }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                    if processor_name == 'success_log_message':
                        update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "success",
                                    "log-message": "successfully moved the ${filename} to bucket"
                                }
                            }
                        },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }

                    if processor_name == 'failed_log_message':
                        update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "error",
                                    "log-message": "error while moving the ${filename} to bucket"
                                }
                            }
                        },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                if processor_group_name == 'UploadToErrorS3':
                    if processor_name == 'GetFile':
                        update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "concurrentlySchedulableTaskCount": "1",
                                    "schedulingPeriod": "* * * * * ?",
                                    "schedulingStrategy": "CRON_DRIVEN",
                                    "properties": {
                                        "Input Directory": "/error_data"
                                    },
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                    if processor_name == 'PutS3Object':
                        update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                     "properties": {
                                        "Bucket": error_bucket_name,
                                        "Access Key": s3_access_key,
                                        "Secret Key": s3_secret_key
                                     }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                    if processor_name == 'success_log_message':
                        update_processor_property_body = {"component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "autoTerminatedRelationships": [
                                        "success"
                                    ],
                                    "properties": {
                                        "log-prefix": "success",
                                        "log-message": "successfully moved the ${filename} to bucket"
                                    }
                                }
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }

                    if processor_name == 'failed_log_message':
                        update_processor_property_body = {"component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "autoTerminatedRelationships": [
                                        "success"
                                    ],
                                    "properties": {
                                        "log-prefix": "error",
                                        "log-message": "error while moving the ${filename} to bucket"
                                    }
                                }
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }

                elif processor_name == 'checking_in_processing':
                    update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "properties": {
                                        "File to Fetch": "/processing_data/${filename}"
                                    }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }

                elif processor_name == 'putfile_processing':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "Directory": "/processing_data/"
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }

                elif processor_name == 'processed_file_error':
                    update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "error",
                                    "log-message": "error while processing ${filename}"
                                }
                            }
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'call_api':
                    update_processor_property_body ={
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "autoTerminatedRelationships": [
                                        "Original"
                                    ],
                                    "properties": {
                                        "HTTP Method": "PUT",
                                        "Remote URL": "https://cqube-dev.tibilprojects.com/ingestion/file-status",
                                        "Content-Type": "application/json"
                                    }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                elif processor_name == 'ExtractText':
                    update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "properties": {
                                        "status": "(?s)(^.*$)"
                                    }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                elif processor_name == 'processed_to_archived':
                    update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "autoTerminatedRelationships": [
                                        "original"
                                    ],
                                    "properties": {
                                        "Command Arguments": "/processing_data;/archived_data",
                                        "Command Path": "mv"
                                    }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": "false"
                        }
                elif processor_name == 'api_badgateway_error':
                    update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "error",
                                    "log-message": "retrying the api call got ${invokehttp.status.code}"
                                }
                            }
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'error_api_call':
                    update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "error",
                                    "log-message": "error while calling the api"
                                }
                            }
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'route_status':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {

                                "properties": {
                                    "file_status": "${status:contains('true')}"
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'failed_toroute_status':
                    update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "error",
                                    "log-message": "routing on status is failed for ${filename}"
                                }
                            }
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'routing_error_status':
                    update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "error",
                                    "log-message": "error while moving the ${filename} from processed to archived"
                                }
                            }
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'success_routing_status':
                    update_processor_property_body = {"component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "success"
                                ],
                                "properties": {
                                    "log-prefix": "error",
                                    "log-message": "succesfully!!! moved the ${filename} to archived "
                                }
                            }
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                # API call to update the processor property
                update_processor_res = rq.put(
                    f"http://{nifi_host}:{nifi_port}/nifi-api/processors/{i['component']['id']}",
                    json=update_processor_property_body)
                if update_processor_res.status_code == 200:
                    print(f"Successfully updated the properties in the {processor_name}")
                    return True

                else:
                    return update_processor_res.text