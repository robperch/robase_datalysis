## MODULE WITH AWS UTILS





"----------------------------------------------------------------------------------------------------------------------"
############################################# Imports ##################################################################
"----------------------------------------------------------------------------------------------------------------------"


"--- Standard library imports ---"
import os
import logging
import pickle

"--- Third party imports ---"
import boto3
from botocore.exceptions import ClientError

"--- Local application imports ---"
from pkg_dir.src.utils import read_yaml
from pkg_dir.config import *





"----------------------------------------------------------------------------------------------------------------------"
############### AWS general functions ##################################################################################
"----------------------------------------------------------------------------------------------------------------------"


"--------------- Unitary functions ---------------"

## Create session from locally stored credentials
def create_aws_session_from_local_yaml():
    """
    Create session from locally stored credentials

    :return aws_ses: (boto3.session.Session) aws session to interact with various aws services
    """


    ## Reading yaml file
    creds = read_yaml(creds_file_path)

    ## Creating session based on credentials
    aws_ses = boto3.Session(
        aws_access_key_id=creds['aws']['aws_access_key_id'],
        aws_secret_access_key=creds['aws']['aws_secret_access_key'],
    )


    return aws_ses






"----------------------------------------------------------------------------------------------------------------------"
############### S3 functions ###########################################################################################
"----------------------------------------------------------------------------------------------------------------------"


"--------------- Unitary functions ---------------"

## Setting up s3 client
def create_s3_client():
    """
    Setting up s3 client

    :return s3_client: (botocore.client.s3) boto3 client to interact with s3
    """


    ## Creating session
    aws_ses = create_aws_session_from_local_yaml()

    ## Create client
    s3_client = aws_ses.client('s3')


    return s3_client



## Uploading file to s3 bucket
def upload_file_to_s3(file_path, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_path: (string) path to the file that will be uploaded
    :param bucket: (string) name of the bucket where the file will be uploaded
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """


    ## If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_path)

    ## Setting up s3 client
    s3_client = create_s3_client()


    ## Upload the file

    try:
        response = s3_client.upload_file(file_path, bucket, object_name)

    except ClientError as e:
        logging.error(e)
        print("Error uploading file to AWS bucket")
        return False

    print("Successfully uploaded file to AWS ({})".format(object_name))


    return True



## List only the objects in a bucket path based on a defined key
def list_objects_in_bucket_key(bucket_name, bucket_key):
    """
    List only the objects in a bucket path based on a defined key

    :param bucket_name: (string) name of the bucket where the objects are
    :param bucket_key: (string) key to locate the objects in the bucket
    :return objects_list: (list) list of objects contained in the bucket based on a key
    """


    ## Setting up s3 client
    s3_client = create_s3_client()

    ## Generating list containing only the object name
    objects_list = [
        obj['Key'].split(sep='/')[-1]
        for obj
        in s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_key)['Contents']
    ]


    return objects_list



## Read object stored in s3 directly into a python variable
def read_s3_obj_to_variable(bucket_name, bucket_key, object_name):
    """
    Read object stored in s3 directly into a python variable

    :param bucket_name: (string) name of the bucket where the objects are
    :param bucket_key: (string) key to locate the objects in the bucket
    :param object_name: (string) name of the object that will be read into a variable
    :return obj_var: (unknown) object's body stored in variable
    """


    ## Setting up s3 client
    s3_client = create_s3_client()

    ## Path to obtain the object from as s3 bucket
    object_path = os.path.join(bucket_key, object_name)

    ## Get object from s3
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_path)

    ## Read object
    obj_var = pickle.loads(obj['Body'].read())


    return obj_var





"--------------- Compounded functions ---------------"





"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
############################################# END OF FILE ##############################################################
"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
