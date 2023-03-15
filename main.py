import boto3

s3_client = boto3.client("s3")

class web_loader():
    '''
    loads data from web, stores file in S3, transfers to RDS or Redshift
    '''

    def __init__(self,
                 bucket):

        self.bucket = bucket

        print(self.bucket)