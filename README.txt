Cloud Function
  1.- Create the cloud function, the steps are within this link from step 1 to 4: https://www.notion.so/deltasmith/Create-New-Data-Source-Stripe-c10bb427278e4451bf00ee3d0ecc8378

  2.- Update the next files in the cloud function:
    -main.py
    -requirements.txt
    -config.json
    -state.json
    -catalog.json
    -catalog_campaigns.json

    2.1 - Make sure to add the service account to the variable "gcp_service_account" and point correctly the next variables to the project name, data set and table

          client_bq = bigquery.Client(credentials=credentials,project='eckhart-tolle-2022')
          table_id_metrics = client_bq.get_table('eckhart-tolle-2022.klaviyo.metrics_api')
          table_id_campaign = client_bq.get_table('eckhart-tolle-2022.klaviyo.campaigns_api')
    
    2.2 - In the line where the load "job_config_metrics" variable is declared, update the the next field as is showed: write_disposition='WRITE_TRUNCATE' 
          This is since there is an error in the big query tables the first time the cloud function sends the information to the tables. Once you run the first test,               you can go and change the field back to write_disposition='WRITE_APPEND' so the next time the function will run it will append the new rows in the table.

  3.- Test the function

Big Query
  1.- Create a table called campaigns_api using the schema_campaigns.json file
  2.- Create a table called metrics_api using the shema_metrics.json file
  
