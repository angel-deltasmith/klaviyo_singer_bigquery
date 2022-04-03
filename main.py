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
    
    today = date.today() #- timedelta(days=1)
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
    data["bookmarks"]["dropped_email"]["since"] = date_new
    data["bookmarks"]["update_email_preferences"]["since"] = date_new
    data["bookmarks"]["receive"]["since"] = date_new
    
    file_temp = tempfile.NamedTemporaryFile()
    file_name = file_temp.name

    with open(file_name, 'w') as outfile:
        json.dump(data, outfile)
    
    command_campaigns = "tap-klaviyo --config ./config.json --state "+ file_name +" --catalog ./catalog_campaigns.json > /tmp/campaign_raw_data.json"
    
    command_metrics = "tap-klaviyo --config ./config.json --state "+ file_name +" --catalog ./catalog.json > /tmp/metrics_raw_data.json"


    status_campaigns = os.system(command_campaigns)
    status_metrics = os.system(command_metrics)
    if status_campaigns == 0 and status_metrics == 0:
        run_process = 0
    else:
        run_process = 1

    if run_process == 0:
        print("SUCCESSFUL LOADED DATA")
        gcp_service_account = """

        """
        json_updated_gcp_service_account = gcp_service_account.replace("'", "\"")
        json_gcp_service_account_key= json.loads(json_updated_gcp_service_account, strict=False)

        #Credentials 
        credentials = service_account.Credentials.from_service_account_info(json_gcp_service_account_key)

        client_bq = bigquery.Client(credentials=credentials,project='eckhart-tolle-2022')

        #Variables
        table_id_metrics = client_bq.get_table('eckhart-tolle-2022.klaviyo.metrics_api')
        table_id_campaign = client_bq.get_table('eckhart-tolle-2022.klaviyo.campaigns_api')

        clean_file_campaign = []
        clean_file_metrics = []
        attribute_fields = ["$message","attributed_event_id"]
        event_properties_fields = ["_cohortvariation_send_cohort","$variation","$event_id","Campaign Name","ClientName","ClientOSFamily","$message","$flow","ClientType","URL","$_cohort$message_send_cohort","message_interaction","ClientOS","$attribution","ESP","Email Domain","List","Subject"]
        record_fields = ["template_id","message_type","campaign_type","num_recipients","status_id","excluded_lists","status_label","name","datetime","status","from_name","updated","is_segmented","send_time","lists","object","statistic_id","event_name","event_properties","from_email","id","timestamp","created","sent_at","subject","person"]
        person_fields = ["id","$address1","$address2","$city","$country","$region","$latitude","$longitude","$zip","$first_name","$email","$last_name","$timezone","$source","Accepts Marketing","created","updated"]
        
        #Cleaning file
        print("INFO:CLEANING CAMPAIGNS FILE")
        print("----------------------------------")
        with open('/tmp/campaign_raw_data.json') as f:
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
                    
                    clean_file_campaign.append(element)
            
            #SAVING FILE IN BIG QUERY
            print("INFO: SAVING CAMPAIGNS DATA IN BIG QUERY ")
            print("----------------------------------")
            job_config_campaign = bigquery.LoadJobConfig(
            autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition='WRITE_TRUNCATE')

            load_job_campaigns = client_bq.load_table_from_json(clean_file_campaign, table_id_campaign, job_config=job_config_campaign, timeout =540 )
            print(load_job_campaigns.result())
            print("INFO: CAMPAIGNS DATA SAVED IN BIG QUERY ")
            print("----------------------------------")

        print("INFO:CLEANING METRICS FILE")
        print("----------------------------------")
        with open('/tmp/metrics_raw_data.json') as metrics:
            data_dict = ndjson.load(metrics)
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
                                    
                                    elif key_record == "person":
                                        element_person = {}
                                        for key_person,value_person in value_record.items():  
                                            
                                            if key_person in person_fields:
                                                key_person = key_person.replace(" ","")
                                                key_person = key_person.replace("$","") 
                                                element_person[key_person] = value_person                                                    
                                        element_record[key_record] = element_person     

                                    else:
                                        element_record[key_record] = value_record

                                element[key_item] = element_record 
                        else:
                            element[key_item] = value_item
                    
                    clean_file_metrics.append(element)
            
            #SAVING FILE IN BIG QUERY
            print("INFO:SAVING METRICS DATA IN BIG QUERY ")
            print("----------------------------------")
            job_config_metrics = bigquery.LoadJobConfig(
            autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition='WRITE_APPEND')
            
            output = ndjson.dumps(clean_file_metrics)
                     
            load_job_metrics = client_bq.load_table_from_json(clean_file_metrics, table_id_metrics, job_config=job_config_metrics, timeout =540 )
            print(load_job_metrics.result())
            
            print("INFO: METRICS DATA SAVED IN BIG QUERY ")
            print("----------------------------------")

if __name__ == "__main__":
    print('Running function from command line.')
    start()
    print('Done.')


def start_api_calls(data, context):
    print('Running google cloud function.')
    start()
    print('Done.')
