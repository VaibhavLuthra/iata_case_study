import json
import boto3
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("event: ", event)
    
    bucket_name = event['bucket_name']
    zip_file_key = event['raw_file_location']
    uncompressed_file_location = event.get('uncompressed_file_location', 'uncompressed/')
    
    csv_file_key = event['csv_file_key']
    parquet_file_key = event.get('parquet_file_key', 'parquet_data/sales.parquet')  # Ensure this includes the file name
    parquet_file_prefix = event['parquet_file_prefix']
    source_location = event['source_location']
    archive_location = event['archive_location']
    

    try:
        csv_content = read_csv_from_s3(bucket_name, csv_file_key)
        convert_csv_to_parquet(csv_content, bucket_name, parquet_file_prefix)
        archive_source_file(bucket_name, source_location, archive_location)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Job completed successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def read_csv_from_s3(bucket_name, csv_file_key):
    print(f"Reading CSV file from s3://{bucket_name}/{csv_file_key}")
    response = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)
    csv_content = response['Body'].read().decode('utf-8')
    print("Reading completed")
    return csv_content
    
def convert_csv_to_parquet(csv_content, bucket_name, parquet_file_prefix):
    df = pd.read_csv(io.StringIO(csv_content))
    path = 's3://iata-case-study/sales_parquet/'
    
    # Convert CSV content to DataFrame
    print("Converting CSV content to DataFrame")
    df = pd.read_csv(io.StringIO(csv_content))
    print("CSV read completed")

    # Write the DataFrame to Parquet files partitioned by Country
    print("Saving DataFrame to Parquet partitioned by Country")
    # Use the 'pyarrow' engine for partitioning
    table = pa.Table.from_pandas(df)
    
    # Specify the target directory for S3
    for country, group in df.groupby('Country'):
        # country_path = f"{parquet_file_prefix}{country}/"
        country_path = f"/tmp/{country}/"
        
        os.makedirs(country_path, exist_ok=True)
        
        # Save each group as a partitioned parquet file
        pq.write_to_dataset(
            table=pa.Table.from_pandas(group),
            root_path=country_path,
            partition_cols=['Country']
        )
        
        for dirpath, _, files in os.walk(country_path):
            for file in files:
                file_path = os.path.join(dirpath, file)
                print("file path: ", file_path)
                s3_key = f"{parquet_file_prefix}country={country.replace(' ', '_').lower()}/{file}"
                print("s3 key: ", s3_key)
                s3_client.upload_file(file_path, bucket_name, s3_key)
                
def archive_source_file(bucket_name, source_location, archive_location):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_location)
    
    for obj in response['Contents']:
        source_key = obj['Key']
        print(source_key)
        # Create the target key by replacing the source prefix with the target prefix
        target_key = source_key.replace(source_location, archive_location, 1)
        print(f"target_key: {target_key}")
    
        # Copy the object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=target_key
        )
    
    print(f"file {source_key} copied to {archive_location} folder")
    