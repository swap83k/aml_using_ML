import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd


@functions_framework.http
def run_predictions(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    client = bigquery.Client()   

    query_job = client.query("""
     SELECT 
        *
        FROM
        ML.PREDICT(MODEL `aml_schema.predict_transactions`, (
        SELECT
        *
        FROM
        `aml_schema.aml_input_predict`
        )) """)
        
   # results = query_job.result()

    bucket_name = "aml_results"
    destination_blob_name = "output.csv"
    print(destination_blob_name)

    rows_df = query_job.result().to_dataframe()
    print(rows_df)
	
    updated_df = rows_df[["customer_id","transaction_date","trasnaction_amount","predicted_alert_flag"]]
	
    storage_client = storage.Client() # Storage API request
    bucket = storage_client.get_bucket(bucket_name) # change the bucket name
    print(bucket)
    blob = bucket.blob('temp/aml_pred_result.csv')
    blob.upload_from_string(updated_df.to_csv(sep=',',index=False,encoding='utf-8'),content_type='application/octet-stream')
    #blob = bucket.blob('temp/output.pdf')
    #blob.upload_from_filename('temp/output.pdf')
    print('Loaded into bucket!')
    
    query_job = client.query("""
      UPDATE `aml_schema.aml_input_predict` AS TableA
SET TableA.alert_flag = TableB.predicted_alert_flag
FROM ( SELECT 
        customer_id,predicted_alert_flag
        FROM
        ML.PREDICT(MODEL `aml_schema.predict_transactions`, (
        SELECT
        *
        FROM
        `aml_schema.aml_input_predict`
        )) ) as TableB
 where tableA.customer_id=TableB.customer_id """)
        
    results = query_job.result()   
    

    returnString = "Prediction success. Predicted results are stored in Bucket :: " + bucket_name
    return returnString