Locally
1.- Clone the repository and run the next command in your local machine: tap-klaviyo --config config.json --discover > catalog.json. This will create a catalog with all the tables you can download from singer, replace the metadata field in each table you want to download, "selected": true will allow the code to download the table, if "selected": false the table won't be downloaded :
        
        "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "table-key-properties": "id",
            "forced-replication-method": "INCREMENTAL",
            "inclusion": "available",
            "selected": true
          }
        }
        
        In this case the tables are clicks, open, subscribe_list, unscubscribe.
        
2.- Duplicate the file and leave the campaigns table with the "selected" option set to "true", the others tables leave them set to false. Rename that file            as: catalog_campaigns.json
        
        "stream": "campaigns",
              "tap_stream_id": "campaigns",
              "key_properties": [
                "id"
              ],
              "metadata": [
                {
                  "breadcrumb": [],
                  "metadata": {
                    "table-key-properties": "id",
                    "forced-replication-method": "FULL_TABLE",
                    "inclusion": "available",
                    "selected": true
                  }
            }
            
Cloud Function
  1.- Create the cloud function, the steps are within this link from step 1 to 4: https://www.notion.so/deltasmith/Create-New-Data-Source-Stripe-c10bb427278e4451bf00ee3d0ecc8378
  
  2.- Upload the next files in the cloud function:
    -main.py
    -requirements.txt
    -config.json 
    -state.json ---> In this file make sure to change the api_key from the new company
    -catalog.json ---> The one you obtain running the command in the Locally step 1
    -catalog_campaigns.json ---> The one you changed in the Locally step 2

    2.1 - Make sure to add the service account to the variable "gcp_service_account" and point correctly the next variables to the project name, data set and table
          
          gcp_service_account= """ SERVICE ACCOUNT STRING """"
          
          client_bq = bigquery.Client(credentials=credentials,project='eckhart-tolle-2022')
          table_id_metrics = client_bq.get_table('eckhart-tolle-2022.klaviyo.metrics_api')
          table_id_campaign = client_bq.get_table('eckhart-tolle-2022.klaviyo.campaigns_api')
    
    2.2 - In the line where the load "job_config_metrics" variable is declared, update the the next field as is showed: write_disposition='WRITE_TRUNCATE' 
          This is since there is an error in the big query tables the first time the cloud function sends the information to the tables. Once you run the first test,             you can go and change the field back to write_disposition='WRITE_APPEND' so the next time the function will run it will append the new rows in the table.
  Big Query  
    1.- Create a table called campaigns_api using the schema_campaigns.json file
    2.- Create a table called metrics_api using the shema_metrics.json file
    3.- Test the function
    
    3.- Test the cloud function



  
