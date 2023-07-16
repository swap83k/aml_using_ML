import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd




@functions_framework.http
def train_model(request):
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
    CREATE OR REPLACE MODEL
				  `aml_schema.predict_transactions`
				OPTIONS
				  ( model_type='LOGISTIC_REG',
          WARM_START=TRUE,
					auto_class_weights=TRUE,
					input_label_cols=['alert_flag']
				  ) AS
				SELECT
				  * 
				FROM
				  `aml_schema.aml_training_view`
        """)
        
    results = query_job.result() 

    print('Model got trained again!!')
    

    query_job = client.query("""
    select * from ml.training_info(MODEL `aml_schema.predict_transactions`);
        """)
        
   # results = query_job.result()     
    rows_df = query_job.result().to_dataframe()
    print(rows_df)
    
    #output = StringIO.StringIO()
    #rows_df.to_csv(output)

    #return Response(output.getvalue(), mimetype="text/csv")
    
    
    return 'Model got trained again!!'