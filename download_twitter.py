import boto3
import os
import botocore



aws_access_key_id=''
aws_secret_access_key=''
client = boto3.client('s3', aws_access_key_id= aws_access_key_id,
    aws_secret_access_key= aws_secret_access_key)
resource = boto3.resource('s3',aws_access_key_id= aws_access_key_id,
    aws_secret_access_key= aws_secret_access_key)



resp = client.list_objects_v2(Bucket= 'kaggletest', Prefix = '2019/08/11') 

for obj in resp['Contents']:
    key = obj['Key']
    temp= key.split('/')
    filename=temp[-1]
    #to read s3 file contents as String
    response = client.get_object(Bucket="kaggletest", Key=key)
    client.download_file('kaggletest', key, filename)


