import json
import boto3
import zipfile
import io

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    # Get bucket name and file key from the event
    print("event: ", event)
    
    bucket_name = event['bucket_name']
    zip_file_key = event['raw_file_location']
    output_prefix = event.get('uncompressed_file_location', 'uncompressed/')

    try:
        compressed_file = get_compressed_file(bucket_name, zip_file_key)
        uncompress_and_save_to_s3(compressed_file, output_prefix, bucket_name)
        response_payload = call_next_job('save_to_parquet', event)
        
        return {
            'statusCode': 200,
            'next job response': response_payload,
            'body': json.dumps('Job completed successfully!')
        }
    except Exception as e:
        return {
            'body': json.dumps({'error': str(e)})
        }


def get_compressed_file(bucket_name, zip_file_key):
        # Download the ZIP file from S3
        print("reading zip file content")
        zip_obj = s3_client.get_object(Bucket=bucket_name, Key=zip_file_key)
        zip_file_content = zip_obj['Body'].read()
        print("completed")
        
        return zip_file_content
        
def uncompress_and_save_to_s3(compressed_file, output_prefix, bucket_name):
    # Unzip the file
    with zipfile.ZipFile(io.BytesIO(compressed_file)) as zip_file:
        for file_info in zip_file.infolist():
            print("file_info: ", file_info)
            # Read each file in the ZIP
            with zip_file.open(file_info) as extracted_file:
                file_data = extracted_file.read()
                print("reading of the content done")

                # Prepare the S3 key for the uncompressed file
                file_name = file_info.filename.replace(' ', '_').lower()
                output_file_key = f"{output_prefix}/{file_name}"
                print("output_file_key: ", output_file_key)
                
                # Upload the uncompressed file back to S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=output_file_key,
                    Body=file_data
                )
                
                print('file saved to S3 at location: ', output_file_key)
                
def call_next_job(job_name, payload):
    response = lambda_client.invoke(
            FunctionName=job_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)  # Pass any necessary payload
        )
    
    print(f"{job_name} function invoked successfully. {response}")
    response_payload = json.loads(response['Payload'].read())
    return response_payload