import json
import pandas as pd
import requests
import boto3

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    # TODO implement
    print("event: ", event)
    
    try:
        url = event['url']
        bucket_name = event['bucket_name']
        file_name = event['raw_file_location']
        
        content = get_data(url)
        upload_to_s3(bucket_name, file_name, content)
        response_payload = call_next_job('uncompress_file', event)
        
        return {
            'statusCode': 200,
            'next job response': response_payload,
            'body': json.dumps('Job completed successfully!')
        }
    except Exception as e:
        return {
            'body': json.dumps({'error': str(e)})
        }

def get_data(url):
    """
    This function uses requests library to download the zip file from given URL
    input: url to download the file from
    output: content of response received from url
    """
    print("reading from URL: ", url)
    response = requests.get(url, headers={"User-Agent": "XY"})
    print("reading completed, status: ", response.raise_for_status())
    
    return response.content
    
def upload_to_s3(bucket_name, file_name, data):
    """
    This function uses requests library to download the zip file from given URL
    input: bucket_name, file_name and data
    output: saves file on S3
    """
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=data)
        print(f"Successfully uploaded {file_name} to {bucket_name}")
    except NoCredentialsError:
        print("Credentials not available.")
        
def call_next_job(job_name, payload):
    response = lambda_client.invoke(
            FunctionName=job_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)  # Pass any necessary payload
        )
    
    print(f"{job_name} function invoked successfully. {response}")
    response_payload = json.loads(response['Payload'].read())
    return response_payload