import boto3 as boto3
import botocore
import uuid
import os

bucket_name = "python-practice-jamesg-run2"
number_of_objects = 10
search_pattern = "test.txt"
createBucketandObjects = True
findObjectsByPattern = False

# S3 Resource/Object instanciation
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

# For pagination
paginator = s3_client.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket_name)

def create_bucket_static(bucket_name, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = bucket_name
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': current_region})
    return bucket_name, bucket_response

def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name

def create_file():
    file_name = create_temp_file(300, search_pattern, 'f')
    return file_name

def find_file(pattern):
    objects = page_iterator.search(f"Contents[?contains(Key, '{pattern}')][]")
    return objects

def remove_local_file(filename):
    os.remove(filename)
    print(f'Removed filename: {filename}')

def createBucketObjects():
    for _ in range(number_of_objects):
        file_name = create_file()
        object = s3_resource.Object(
            bucket_name=bucket_name, 
            key=file_name
        )
        object.upload_file(file_name)
        print(f'Uploading file: {file_name}')
        remove_local_file(file_name)

if createBucketandObjects == True:
    # Create bucket it doesn't exist
    try:
        bucket = s3_resource.Bucket(bucket_name)
        if bucket.creation_date:
            createBucketObjects()
        else:
            print(f"The bucket does not exist, creating... {bucket_name}")
            create_bucket_static(bucket_name, s3_resource.meta.client)
            createBucketObjects()
    except botocore.exceptions.ClientError as error:
        print('Error while creating bucket', error)
    finally:
        print('Finished succesfully.')

if findObjectsByPattern == True:
    try:
        bucket = s3_resource.Bucket(bucket_name)
        objects = find_file(search_pattern)
        for item in objects:
            print('Name of object: ' + item['Key'])
            print('Last Modified: ' + str(item['LastModified']))
            print('eTag: ' + item['ETag'])
            print('Size: ' + str(item['Size']))
            print('Storage Class: ' + item['StorageClass'])
    except botocore.exceptions.ClientError as error:
        print('Error while creating bucket', error)
    finally:
        print('Finished succesfully.')