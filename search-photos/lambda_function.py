import json
import boto3
import json
import inflection
from opensearchpy import OpenSearch, RequestsHttpConnection

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')
HOST = 'search-photos-cf-a7cn7yvm7f3rvspzngd53jctty.us-east-1.es.amazonaws.com'
REGION = 'us-east-1'
INDEX = 'photos'

def lambda_handler(event, context):
    # TODO implement
    print(event)
    query = event['queryStringParameters']['q']
    print(f"Message from frontend: {query}")
    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='HU3VCFSG0U', # MODIFY HERE
            botAliasId='TSTALIASID', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=query
    )
    
    msg_from_lex = response.get('messages', [])
    interpretations = response['interpretations']
    
    for inter in interpretations:
        if inter['intent']['name'] == 'SearchIntent':
            query_term1 = inter['intent']['slots']['query_term1']
            query_term2 = inter['intent']['slots']['query_term2']
    
    print(query_term1)
    print(query_term2)
    
    # Return empty array if no terms detected
    photos = []
    
    # OpenSearch
    search_term = ''
    if query_term1:
        search_term += inflection.singularize(query_term1['value']['interpretedValue'])
    if query_term2:
        search_term += ' '
        search_term += inflection.singularize(query_term2['value']['interpretedValue'])
    print(search_term)
    
    response = search(search_term.lower())
    photos += response
    print(photos)
    
    signed_urls = []
    
    for photo in photos:
        url = boto3.client('s3').generate_presigned_url(
              ClientMethod='get_object', 
              Params={'Bucket': photo['bucket'], 'Key': photo['object_key']},
              ExpiresIn=3600)
        signed_urls.append(url)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'
        },
        'body': json.dumps({'results': signed_urls})
    }

def search(term):
    q = {'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
            'host': HOST,
            'port': 443
        }],
        http_auth=('master', 'Columbia@12'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results
    