import json
from csv import DictReader
import os
import tempfile
from decimal import Decimal

def lambda_handler(event, context):
   
    bucket = event['Records'][0]['s3']['bucket']['name']
    print(bucket)
    if bucket != "test-rx-bucket":

        import boto3
        s3 = boto3.client('s3')
        ddb = boto3.resource('dynamodb')
        key = event['Records'][0]['s3']['object']['key']
        print(key)
        
        key_prefix = key[0:(key.find("/"))]
        
        print(key_prefix)
        
        try:
            if key_prefix == "assessment":
                tmpdir = tempfile.mkdtemp()
                saved_umask = os.umask(0o77)
                download_path = os.path.join(tmpdir, "assessment.csv")
                ddb_table_name = "ProdAssessment"
                
                ddb_table_performancemeasures = "PerformanceMeasures"
                ddb_table_answers = "Answers"
            elif key_prefix == "prescription":
                tmpdir = tempfile.mkdtemp()
                saved_umask = os.umask(0o77)
                download_path = os.path.join(tmpdir, "prescription.csv")
                ddb_table_name = "ProdPrescription"
            elif key_prefix == "activity":
                tmpdir = tempfile.mkdtemp()
                saved_umask = os.umask(0o77)
                download_path = os.path.join(tmpdir, "activity.csv")
                ddb_table_name = "ProdActivity"
            elif key_prefix == "feedback":
                tmpdir = tempfile.mkdtemp()
                saved_umask = os.umask(0o77)
                download_path = os.path.join(tmpdir, "feedback.csv")
                ddb_table_name = "ProdFeedback"
            elif key_prefix == "question":
                tmpdir = tempfile.mkdtemp()
                saved_umask = os.umask(0o77)
                download_path = os.path.join(tmpdir, "question.csv")
                ddb_table_name = "ProdQuestion"
            elif key_prefix == "assessmentsummary":
                tmpdir = tempfile.mkdtemp()
                saved_umask = os.umask(0o77)
                download_path = os.path.join(tmpdir, "question.csv")
                ddb_table_name = "ProdAssessmentSummary"
            else:
                assert (True), "Not a valid Key prefix! Current value = " + key_prefix
        except AssertionError as e:
            print(e)
            raise e
        
        
        try:
            s3.download_file(bucket, key, download_path)
            print("File Downloaded")
        except Exception as e:
            print(e)
            print("Error downloading S3 object %s from bucket %d.")
            raise e
    
        ddb_table = ddb.Table(ddb_table_name)
    else:
        download_path = 'athena_rx_query_output.csv' ### update
        ddb_table_name = "ProdPrescription"
        key_prefix = 'prescription'

    items = []
    ddb_table_modifiedat = "ModifiedAt"
    ddb_table_userid = "UserId"
    csv_header_hash = "user_id"
    ddb_table_event = "EventName"
    ddb_table_data = "data"
    ddb_table_prescripid = "PrescriptionId"
    ddb_table_assessid = "AssessmentId"
    
    print("Writing Athena query output into DynamoDB table " + ddb_table_name + "\n")
    
    try:        
        print("attempting to create JSON")
        # Iterate over CSV file and generates Items list
        with open(download_path, 'r') as csv_file:
            dict_reader = DictReader(csv_file)
            records_list = list(dict_reader)
            for record in records_list:
                if 'json' in record.keys():
                    record['data'] = record.pop('json')
                if record not in items:
                    items.append(record)

    except Exception as e:
        print(e)
        print("Error working in file:", download_path)
        raise e
    # else:
    #     os.remove(download_path)
    # finally:
    #     os.umask(saved_umask)
    #     os.rmdir(tmpdir)
    
    # print("JSON Created, Beginning Write Process")
    
    # with ddb_table.batch_writer() as batch:
    #     print("batch_writer created")
    #     length = len(items)
    #     print(length)
    #     for item in items:
    #         try:
    #             print("attempting write")
    #             batch.put_item(Item=item)
    #             print('Writing data for item:')
    #             print(item)
    #         except Exception as e:
    #             print(e)
    #             print('Error writing data for item:')
    #             print(item)
    #             raise e
    
    return {
        'statusCode': 200,
        'body': json.dumps('dynamodb_import Lambda complete!')
    }