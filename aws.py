import boto3
import json

lambda_client = boto3.client('lambda')
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
sts_client = boto3.client('sts')


# %% AWS SECRET
def get_secret(secret_name, region_name = "us-east-1" ):
    known_secret = []
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    secret = json.loads(get_secret_value_response['SecretString'])
    secret['ARN'] = get_secret_value_response['ARN']
    return secret

# %% Redshift Client
def statement_result_to_df(response):
    import pandas as pd
    colNames = [i['name'] for i in response['ColumnMetadata']]
    records = []
    for i,irec in enumerate(response['Records']):   
        record = {}
        for j, jrec in enumerate(irec):
            record[colNames[j]] = list(jrec.values())[0]
        records.append(record)
    df = pd.DataFrame(data = records)
    return df
# %% SNS
def send_to_sns(event, TargetArn):
    sns_client = boto3.client('sns')
    # 
    
    responseMessage = json.dumps(str(event))
    print(responseMessage)
    print(type(responseMessage))
    response = sns_client.publish (
        TargetArn = TargetArn,
        Message = responseMessage,
        MessageStructure = 'str'
    )