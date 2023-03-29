import json
import boto3
import requests
import json
import inflection
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
rek_client = boto3.client('rekognition')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print(event)
    bucket = f"{event['Records'][0]['s3']['bucket']['name']}"
    object_key = f"{event['Records'][0]['s3']['object']['key']}"
    rek_response = {}
    
    # Get Rekognition labels
    try:
        rek_response = rek_client.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': object_key
                }
            }
        )
    except Exception as e:
        print(e)
        print("Unable to get labels from Recoknition")
    
    # Get customer labels
    metadata_response = s3_client.head_object(
        Bucket=bucket,
        Key=object_key
    )
    
    if metadata_response['Metadata'] and metadata_response['Metadata']['customlabels']:
        customLabels = metadata_response['Metadata']['customlabels']
        customLabels = customLabels.split(',')
    else:
        customLabels = []
        
    print(customLabels)
    customLabels = [inflection.singularize(x).lower() for x in customLabels]
    if rek_response:
        for label in rek_response['Labels']:
            customLabels.append(inflection.singularize(label['Name']).lower())
        
    print(rek_response)
    print(metadata_response)
    print(customLabels)
    
    # Send index object to opensearch
    index_object = {
        "object_key": object_key,
        "bucket": bucket,
        "createdTimestamp": f"{metadata_response['LastModified'].strftime('%Y-%m-%dT%H:%M:%S')}",
        "labels": customLabels
    }
    print(index_object)

    host = 'search-photos-47vmhipmmar5jhowic4lcpkjzu.us-east-1.es.amazonaws.com'
    region = 'us-east-1'
    index_name = 'photos'
    service = 'es'
    auth = ('master', 'Columbia@12')
    
    search = OpenSearch(hosts=[{
                            'host': host,
                            'port': 443
                        }],
                        http_auth=auth,
                        use_ssl=True,
                        verify_certs=True,
                        ssl_assert_hostname = False,
                        ssl_show_warn = False,
                        connection_class=RequestsHttpConnection)
    
    # add document to index
    add_response = search.index(index=index_name, id=object_key, body=index_object, refresh=True)
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': add_response
    }
