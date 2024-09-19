import json
import boto3

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    print(bucket)
    key = event['Records'][0]['s3']['object']['key']
    print(key)
    
    key_prefix = key[0:(key.find("/"))]
    print(key_prefix)
    
    client = boto3.client('athena')
    
    database = "data-analytics-prod"
    table = ''
    output = ""
    
    try:
        if key_prefix == "assessment":
            table = "assessment"
            output = 's3://listparquetprodresult/assessment/'
        elif key_prefix == "prescription":
            table = "prescription"
            output = "s3://listparquetprodresult/prescription/"
        elif key_prefix == "activity":
            table = "activity"
            output = "s3://listparquetprodresult/activity/"
        elif key_prefix == "feedback":
            table = "feedback"
            output = "s3://listparquetprodresult/feedback/"
        elif key_prefix == "question":
            table = "question"
            output = "s3://listparquetprodresult/question/"
        elif key_prefix == "assessmentsummary":
            table = "assessmentsummary"
            output = "s3://listparquetprodresult/assessmentsummary/"
        else:
            assert (False), "Not a valid Key prefix! Current value = " + key_prefix
    except AssertionError as e:
        print(e)
        raise e

    
    queryString = "SELECT * FROM " + table + " WHERE modifiedat > (current_timestamp - interval '12' hour)"
    
    if key_prefix == "prescription":
        queryString = "SELECT cast (data as json) as json, prescriptionid, data.modifiedat as ModifiedAt FROM prescription WHERE data.modifiedat > (current_timestamp - interval '12' hour)"
    
    
    print(queryString)
    
    queryStart = client.start_query_execution(
        QueryString = queryString,
        QueryExecutionContext = {
            'Database': database
        },
        ResultConfiguration = {
            'OutputLocation' : output
        }
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Athena query Lambda complete.",

        }),
    }
