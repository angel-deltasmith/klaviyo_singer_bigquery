1.- Create the cloud function, the steps are within this link from step 1 to 4: https://www.notion.so/deltasmith/Create-New-Data-Source-Stripe-c10bb427278e4451bf00ee3d0ecc8378

2.- Update the files in this repository
  2.1 - Make sure to add the service account to the variable "gcp_service_account" and point correctly the next variables to the project name, data set and table
        
        client_bq = bigquery.Client(credentials=credentials,project='eckhart-tolle-2022')
        table_id_metrics = client_bq.get_table('eckhart-tolle-2022.klaviyo.metrics_api')
        table_id_campaign = client_bq.get_table('eckhart-tolle-2022.klaviyo.campaigns_api')

3.- Test the function
