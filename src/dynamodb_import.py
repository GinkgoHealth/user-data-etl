import json
import boto3
import csv
import os
import tempfile
from decimal import Decimal


s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb')

def lambda_handler(event, context):
   
    bucket = event['Records'][0]['s3']['bucket']['name']
    print(bucket)
    key = event['Records'][0]['s3']['object']['key']
    print(key)
    
    
    key_prefix = key[0:(key.find("/"))]
    
    print(key_prefix)
    
    ddb_table_name = ""
    download_path = ""
    
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
    
    ddb_table_modifiedat = "ModifiedAt"
    ddb_table_userid = "UserId"
    csv_header_hash = "user_id"
    ddb_table_event = "EventName"
    ddb_table_data = "data"
    ddb_table_prescripid = "PrescriptionId"
    ddb_table_assessid = "AssessmentId"
    
    ddb_table = ddb.Table(ddb_table_name)
    
    print("Writing Athena query output into DynamoDB table " + ddb_table_name + "\n")
    
    try:
        s3.download_file(bucket, key, download_path)
        print("File Downloaded")
    except Exception as e:
        print(e)
        print("Error downloading S3 object %s from bucket %d.")
        raise e

    items = []
    
    try:
        # Initialize the Items list variable
        
        print("attempting to create JSON")
        # Iterate over CSV file and generates Items list
        with open(download_path, 'r') as csv_file:
            csv_obj = csv.reader(csv_file)
            for row in csv_obj:
                if (row[0] != csv_header_hash) and (row[0] != "json"):
                    if key_prefix == "assessment":
                        print("Creating assessment Json")
                        json_item = '{"' + ddb_table_userid + '": "' + row[0] + '", "' + ddb_table_assessid + '": "' + row[3] + '", "' + ddb_table_modifiedat + '": "' + row[2] + '", "' + ddb_table_performancemeasures + '": "'+ row[5] + '", "' + ddb_table_answers + '": "' + row[4] + '", "' + ddb_table_event + '": "' + row[1] + '"}'
                    elif key_prefix == "prescription":
                        print("Creating prescription Json")
                        json_item = '{"' + ddb_table_data + '": ' + row[0] + ', "' + ddb_table_prescripid + '": "' + row[1] + '", "' + ddb_table_modifiedat + '": "' + row[2] + '"}'
                    elif key_prefix == "activity":
                        json_item = '{"' + ddb_table_userid + '": "' + row[0] + '", "' + ddb_table_event + '": "' + row[1] + '", "' + "ActivityId" + '": "' + row[2] + '", "' + "ActivityDate" + '": "' + row[3] + '", "' + "Category" + '": "' + row[4] + '", "' + "ExpiresAt" + '": "' + row[5] + '", "' + "LastCompletedIndex" + '": "' + row[6] + '", "' + "Minutes" + '": "' + row[7] + '", "' + ddb_table_modifiedat + '": "' + row[8] + '", "' + "PrescriptionDay" + '": "' + row[9] + '", "' + "PrescriptionId" + '": "' + row[10] + '", "' + "RPE" + '": "' + row[11] + '", "' + "Type" + '": "' + row[12] + '"}'
                    elif key_prefix == "question":
                        json_item = '{"' + ddb_table_userid + '": "' + row[0] + '", "' + ddb_table_event + '": "' + row[1] + '", "' + ddb_table_modifiedat + '": "' + row[2] + '", "' + "Answer" + '": "' + row[3] + '", "' + "QuestionId" + '": "' + row[4] + '"}'
                    elif key_prefix == "feedback":
                        json_item = '{"' + ddb_table_userid + '": "' + row[0] + '", "' + ddb_table_event + '": "' + row[1] + '", "' + "FeedbackId" + '": "' + row[2] + '", "' + "ExerciseId" + '": "' + row[3] + '", "' + "ExpiresAt" + '": "' + row[4] + '", "' + ddb_table_modifiedat + '": "' + row[5] + '", "' + "PrescriptionId" + '": "' + row[6] + '", "' + "PresciptionWeek" + '": "' + row[7] + '", "' + ddb_table_prescripid + '": "' + row[8] + '"}'
                    elif key_prefix == "assessmentsummary":
                         json_item = '{"' + ddb_table_userid + '": "' + row[0] + '", "' + ddb_table_event + '": "' + row[1] + '", "' + ddb_table_assessid + '": "' + row[2] + '", "' + "ClosedAt" + '": "' + row[3] + '", "' + ddb_table_prescripid + '": "' + row[5] + '", "' + "CreatedTime" + '": "' + row[6] + '", "' + "ModifiedAt" + '": "' + row[1] + row[4] + '"}'
                    if json.loads(json_item) not in items:
                        print(json_item)
                        items.append(json.loads(json_item, parse_float=Decimal))

    except Exception as e:
        print(e)
        print("Error working in file:", download_path)
        raise e
    else:
        os.remove(download_path)
    finally:
        os.umask(saved_umask)
        os.rmdir(tmpdir)
    
    print("JSON Created, Beginning Write Process")
    
    with ddb_table.batch_writer() as batch:
        print("batch_writer created")
        length = len(items)
        print(length)
        for item in items:
            try:
                print("attempting write")
                batch.put_item(Item=item)
                print('Writing data for item:')
                print(item)
            except Exception as e:
                print(e)
                print('Error writing data for item:')
                print(item)
                raise e
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
