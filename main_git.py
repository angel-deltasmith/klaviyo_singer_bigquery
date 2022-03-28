import subprocess
import os
import json
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account
import ndjson 
from datetime import date
from datetime import datetime, timedelta
import tempfile

def start():
    
    today = date.today() #+ timedelta(days=1)
    print("Today's date:", today)
    date_new = today.strftime('%Y-%m-%d') + "T00:00:00Z"
    
    f = open ('state.json', "r")
    data = json.loads(f.read())
    data["bookmarks"]["campaigns"]["since"] = date_new 
    data["bookmarks"]["open"]["since"] = date_new
    data["bookmarks"]["click"]["since"] = date_new
    data["bookmarks"]["subscribe_list"]["since"] = date_new
    data["bookmarks"]["unsubscribe"]["since"] = date_new
    data["bookmarks"]["unsub_list"]["since"] = date_new
    data["bookmarks"]["lists"]["since"] = date_new
    
    file_temp = tempfile.NamedTemporaryFile()
    file_name = file_temp.name

    with open(file_name, 'w') as outfile:
        json.dump(data, outfile)
    
    command = "tap-klaviyo --config ./config.json --state "+ file_name +" --catalog ./catalog_campaigns.json > /tmp/raw_data.json"
    status = os.system(command)

    if status == 0:
        print("SUCCESSFUL LOADED DATA")
        gcp_service_account = """

        """
        json_updated_gcp_service_account = gcp_service_account.replace("'", "\"")
        json_gcp_service_account_key= json.loads(json_updated_gcp_service_account, strict=False)

        #Credentials 
        credentials = service_account.Credentials.from_service_account_info(json_gcp_service_account_key)

        client_bq = bigquery.Client(credentials=credentials,project='project-id')

        #Variables
        table_id = client_bq.get_table('project-id.dataset.table')

        clean_file = []
        attribute_fields = ["$message","attributed_event_id"]
        event_properties_fields = ["_cohortvariation_send_cohort","$variation","$event_id","Campaign Name","ClientName","ClientOSFamily","$message","$flow","ClientType","URL","$_cohort$message_send_cohort","message_interaction","ClientOS","$attribution","ESP","Email Domain","List","Subject"]
        record_fields = ["template_id","message_type","campaign_type","num_recipients","status_id","uuid","excluded_lists","status_label","name","datetime","status","from_name","updated","is_segmented","send_time","lists","object","statistic_id","event_name","event_properties","from_email","id","timestamp","created","sent_at","subject"]
        with open('/tmp/raw_data.json') as f:
            data_dict = ndjson.load(f)
            for item in data_dict:# 5 records
                element = {}
                if item["type"] == "RECORD":
                    for key_item,value_item in item.items():
                        if key_item == "record":
                            element_record = {}
                            for key_record,value_record in value_item.items():      
                                if key_record in record_fields:                 
                                    if key_record == "event_properties":
                                        element_event = {}
                                        for key_event,value_event in value_record.items():   
    
                                            if key_event in event_properties_fields:

                                                key_event = key_event.replace(" ","")
                                                key_event = key_event.replace("$","")
                                                element_attribution = {}    
                                                if key_event == "attribution": 
                                                    for key_attribution,value_attribution in value_event.items(): 

                                                        if key_attribution in attribute_fields:
                                                           
                                                            key_attribution = key_attribution.replace(" ","")
                                                            key_attribution = key_attribution.replace("$","")   
                                                            element_attribution[key_attribution] = value_attribution                                               
                                                    element_event[key_event] = element_attribution
                                                else:
                                                    element_event[key_event] = value_event      

                                        element_record[key_record] = element_event    
                                                            
                                    else:
                                        element_record[key_record] = value_record

                                element[key_item] = element_record 
                        else:
                            element[key_item] = value_item
                    
                    clean_file.append(element)
                    

            output = ndjson.dumps(clean_file)
            #with open('clean_file.json', 'w') as json_file:
                
            #    json_file.write(output)

            job_config = bigquery.LoadJobConfig(
            autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition='WRITE_APPEND')

            load_job = client_bq.load_table_from_json(clean_file, table_id, job_config=job_config, timeout =480 )
            print(load_job.result())


if __name__ == "__main__":
    print('Running function from command line.')
    start()
    print('Done.')

        

