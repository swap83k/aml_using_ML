import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

def load_bq_from_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    lst = []
    file_name = event['name']
    table_name = file_name.split('_')[0]
    table_name = f'aml_input_{table_name}'

    # Event,File metadata details writing into Big Query
    dct={
         'Event_ID':context.event_id,
         'Event_type':context.event_type,
         'Bucket_name':event['bucket'],
         'File_name':event['name'],
         'Created':event['timeCreated'],
         'Updated':event['updated']
        }
    lst.append(dct)
    df_metadata = pd.DataFrame.from_records(lst)
    df_metadata.to_gbq('aml_schema.bq_load_log', 
                        project_id='project_id', 
                        if_exists='append',
                        location='us')
    
    # Actual file data , writing to Big Query
    df_data = pd.read_csv('gs://' + event['bucket'] + '/' + file_name)

    df_data.to_gbq('aml_schema.' + table_name, 
                        project_id='project_id', 
                        if_exists='append',
                        location='us')