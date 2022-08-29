import json
import boto3
import botocore.session as bc
print('Loading function load-redshift-sql-from-s3-v1')
s3_client = boto3.client('s3')

REGION = "ap-southeast-2"
REDSHIFT_DATABASE_NAME = "sample_data_dev"

# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:   
# https://aws.amazon.com/developers/getting-started/python/

import boto3
import base64
from botocore.exceptions import ClientError


# define RedShift secrets
def get_secret():
    secret_name = "redshift-cluster-v1"
    region_name = REGION
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        secret_arn=get_secret_value_response["ARN"]
        print("secret_arn", secret_arn)
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            secret_json = json.loads(secret)
            print("########## secrets secret_json", secret_json)
            cluster_id=secret_json["dbClusterIdentifier"]
            print("secrets cluster_id", cluster_id)
            return secret_arn, cluster_id

secret_arn, cluster_id = get_secret()
print("## secret_arn", secret_arn)
print("## secrets cluster_id", cluster_id)

# create redshift session
bc_session = bc.get_session()
session = boto3.Session(
        botocore_session=bc_session,
        region_name=REGION,
)
# Setup the redshift client
client_redshift = session.client("redshift-data")
print("redshift Data API client successfully loaded")


# get a query from an S3 object
def getQueryFromS3(bucket_name, object_key):
    print("looking for bucket_name", bucket_name, "object_key", object_key)
    file_contents = s3_client.get_object(
        Bucket=bucket_name, Key=object_key)["Body"].read()
    print("file_contents", file_contents)
    query_str = file_contents.decode("utf-8") 
    print("query_str", query_str)
    return query_str


# call redshift execute statement with a query string
def redshiftExecuteStatement(query_str):
    response = client_redshift.execute_statement(Database= REDSHIFT_DATABASE_NAME, SecretArn= secret_arn, ClusterIdentifier= cluster_id, Sql= query_str)
    print("execute_statement response", response)
    response_id = response["Id"]
    print("execute_statement id", response_id)
    return response_id


# call redshift describe statement with a query id
def getQueryStatus(query_id):
    response = client_redshift.describe_statement(Id= query_id)
    print("describe_statement response", response)
    status = response["Status"]
    print("describe_statement status", status)
    return status

    
def lambda_handler(event, context):
    print("Entered load-redshift-sql-from-s3-v1 lambda_handler")
    print("event", event)
    
    # bucket_name and object_key are required
    bucket_name = event["s3_bucket_name"] # "redshift-sql-queries-190067120391"
    object_key = event["s3_object_key"] # "development/sql-queury-alpha-v1.sql"

    # check if the query is already running, otherwise go get it
    if "execute_statement_id" not in event:
        # get query from S3
        query_str = getQueryFromS3(bucket_name, object_key)
        # redshift execute statement using the query
        response_id = redshiftExecuteStatement(query_str)
        event["execute_statement_id"] = response_id
    else:
        # use redshift describe statement to get the query status
        query_status = getQueryStatus(event["execute_statement_id"])
        event["query_status"] = query_status

    return event
