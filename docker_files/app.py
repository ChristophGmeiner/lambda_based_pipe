import boto3
from botocore.exceptions import ClientError
from urllib import request
import requests
import shutil
import os
import logging
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
import ast
import json
import awswrangler as wr

#same as root web_s3_db_la,ba.py

class WebLoader():
    """
    loads data from web, stores file in S3, transfers to RDS or Redshift
    """

    def __init__(self,
                 file_dest_name: str,
                 bucket: str,
                 bucket_dest_folder: str,
                 file_download: bool,
                 file_format: str,
                 tempfolder: str = None,
                 zip_file: bool = False):
        """
        :param file_dest_name: indicating which file to load, e.g. WDI or eea
        :param bucket: destination bucket as string
        :param bucket_dest_folder: foldername for destination in bucket
        :param file_download: Does the wb request lead to a direct file downloa?
        :param tempfolder: local storage destiantion
        :param zip_file: Will the download be a zip file?
        :param file_format: CSV or JSON to further process?
        """

        if tempfolder:
            if tempfolder[0] != "/":
                tempfolder = "/" + tempfolder
            if tempfolder[-1] != "/":
                tempfolder += "/"
            tempfolder = os.getcwd() + tempfolder
            self.tempdir = tempfolder
        else:
            self.tempdir = os.getcwd() + "/"
        self.file_dest_name = file_dest_name
        self.bucket = bucket
        self.bucket_dest_folder = bucket_dest_folder
        if self.bucket_dest_folder[-1] != "/":
            self.bucket_dest_folder += "/"
        self.zip_file = zip_file
        self.file_format = file_format
        self.file_download = file_download
        self.s3_client = boto3.client("s3")
        self.date_folder = date.today().strftime("%Y%m%d") + "/"

        logging.info("Tmp dir: %s" % self.tempdir)

    def create_raw_files(self,
                         store_files: bool = None,
                         file_data_url: str = None,
                         direct_to_bucket: bool = False):
        """
        Create raw files from web request and store those locally in tempfolder from class
        :param file_data_url: URL for file download or creation
        :param store_files: Also store files locally and in S3
        :param direct_to_bucket: skip local filesystem
        :return: string indicating local destination, only in case store_files of class is set to False
        """
        if not direct_to_bucket:
            dest = self.tempdir

            if not store_files:
                logging.info("%s provided for further process" % dest)
                return dest

            if store_files and not file_data_url:
                logging.error("Storing files only possible, if URL is provided")

            if self.file_download:
                if self.zip_file:
                    if store_files:
                        r = request.urlretrieve(file_data_url, dest + self.file_dest_name + ".zip")
                        shutil.unpack_archive(dest + self.file_dest_name + ".zip", dest)
                        logging.info("%s unpacked to %s" % (self.file_dest_name, dest))
                        logging.info(os.listdir(dest))

            else:
                dest_file = dest + "/" + self.file_dest_name + "." + self.file_format
                if store_files:
                    r = requests.get(file_data_url)
                    data = r.json()
                    with open(dest_file, "w") as f:
                        json.dump(data, f)
                    logging.info("Following files stored: ")
                    logging.info(os.listdir(dest))

        else:
            r = requests.get(file_data_url, stream=True)
            data = json.dumps(r.json(), indent=2, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.bucket_dest_folder + self.file_dest_name + "." + self.file_format,
                Body=data
            )

    def list_bucket_files(self,
                          store_to_local_temp: bool = False):
        """
        create a list of bucket files in dest folder on bucket, if nothing is there, get the list from local path
        :param store_to_local_temp: Whether the files shall be transferred form bucket to local temp drive
        :return: list of file names (strings)
        """
        bf_list = list()
        bucket_dest_folder = self.bucket_dest_folder + self.date_folder
        bucket_files = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=bucket_dest_folder
        )
        for bf in bucket_files["Contents"]:
            bf_list.append(bf["Key"])

        if store_to_local_temp:
            source_folder = self.create_raw_files(store_files=False)
            logging.info("Downloading to %s" % source_folder)

            i = 1
            for bf in bf_list:
                local_file_name_short = bf[len(bucket_files["Prefix"]):]
                self.s3_client.download_file(Bucket=self.bucket,
                                             Key=bf,
                                             filename=source_folder + local_file_name_short)
                logging.info("%d of %d files downloaded!" % (i, len(bf_list)))
                i += 1
            local_dir_list = os.listdir(source_folder)
            logging.info("See new temp folder: %s" % local_dir_list)

        return bf_list

    def list_load_files(self,
                        bucket: bool = False):
        """
        Create a list of files for load to databases later
        :param bucket: BTake files from bucket instead of local temp drive
        :return: list of filenames as strings
        """

        if bucket:
            load_list = self.list_bucket_files()

        else:
            source_folder = self.create_raw_files(store_files=False)
            logging.info(source_folder)
            if os.path.isdir(source_folder):
                load_list = os.listdir(source_folder)
                load_list = [source_folder + "/" + f for f in load_list]
            else:
                logging.warning("Local folder is empty ... switching to bucket!")
                load_list = self.list_bucket_files()

        return load_list

    def delete_local_temp_files(self):
        """
        Delete files on local temp drive
        :return: None
        """
        source_folder = self.create_raw_files(store_files=False)
        logging.info("Starting emptying %s" % source_folder)
        shutil.rmtree(source_folder)
        logging.info("Succesfully emptied %s!" % source_folder)
        logging.info("Tempdir now: %s" % os.listdir(self.tempdir))

    def move_raw_files_s3(self,
                          delete_local_files: bool = False,
                          file_prefix: str = None,
                          file_suffix: str = None):
        """
        Move files from local temp drive to S3 bucket
        :param delete_local_files: delete local files after transfer to S3?
        :param file_prefix: File prefix for S3 files
        :param file_suffix: File suffix for S3 files
        :return: None
        """
        bucket_dest_folder = self.bucket_dest_folder + self.date_folder
        source_folder = self.create_raw_files(store_files=False)
        source_files = os.listdir(source_folder)

        i = 1
        for sf in source_files:
            file_name = source_folder + "/" + sf
            object_name = sf
            if file_prefix:
                file_prefix = file_prefix.replace(".", "")
                object_name = sf + file_prefix
            if file_suffix:
                object_name = sf[0:sf.find(".")] + file_suffix + sf[sf.find(".") + 1:]
            object_name = bucket_dest_folder + object_name
            response = self.s3_client.upload_file(file_name, self.bucket, object_name)
            logging.info("%d of %d files finished" % (i, len(source_files)))
            i += 1

        bucket_files = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=bucket_dest_folder
        )
        bf_list = list()
        for bf in bucket_files["Contents"]:
            bf_list.append(bf["Key"])

        logging.info("Bucket new details: %s" % ", ".join(bf_list))

        if delete_local_files:
            self.delete_local_temp_files()

    def get_secret(self,
                   secret_name,
                   region_name="eu-central-1"):
        """
        Request AWS secret details
        :param secret_name: AWS secret name
        :param region_name: AWS region of secret manager
        :return: Secret either as string or list
        """

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e

        secret = get_secret_value_response['SecretString']

        return secret

    def load_db(self,
                secret_name: str,
                files_from_bucket: bool = False,
                type:str = "rds",
                delete_local_files: bool = True,
                redshift_kwargs: dict = None):
        """
        loads files to database
        :param secret_name: name of AWS secret for db creds
        :param files_from_bucket: take files from bucket? if False local temp files are taken
        :param type: database type, RDS or Redshift
        :param delete_local_files: delete local files after complete load to DB?
        :param redshift_kwargs: Redshift load args - see func below for details
        :return: None
        """

        load_list = self.list_load_files(bucket=files_from_bucket)
        logging.info(load_list)

        secrets = self.get_secret(secret_name)

        if len(secrets) > 20:
            secrets = ast.literal_eval(secrets)

        #on non local env can be used with wr and Glue RDS conn
        if type == "rds":
            host = "localhost" #secrets["host"]
            port = "5555" #secrets["port"]
            logging.info("Connecting to %s:%s" % (host, port))
            conn =  "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                secrets["username"],
                secrets["password"],
                host,
                port,
                "postgres"
            )
            engine = create_engine(conn)

        if type == "redshift":
            assert redshift_kwargs["glue_conn"]
            glue_conn = redshift_kwargs["glue_conn"]
            logging.info("Connecting to %s via AWS Wrangler and Glue Conn" % glue_conn)
            conn = wr.redshift.connect(glue_conn)
            logging.info("Connection successful")

        i = 1
        for f in load_list:

            if f[f.find(".") + 1:] != self.file_format:
                raise Exception("File extensions in bucket do not match file_format parameter: %s" % load_list)

            if self.file_format == "json":
                logging.info("Reading %s" % f)
                if not files_from_bucket:
                    with open(f, "r") as fj:
                        df_base = json.load(fj)
                else:
                    r = self.s3_client.get_object(
                        Bucket=self.bucket,
                        Key=self.bucket_dest_folder + self.file_dest_name + "." + self.file_format
                    )
                    df_base = json.loads(r.get("Body").read())
                df_base = df_base["results"]
                df = pd.DataFrame(df_base)
                logging.info(df.head())

            elif self.file_format == "csv":
                logging.info("Loading %s to DF" % f)
                df = pd.read_csv(f,
                                 header=0)

            df = df.dropna(axis=1, how="all")
            table_name = self.file_dest_name + str(i)
            table_name = table_name.lower()
            if type == "rds":
                df.to_sql(table_name,
                          engine,
                          index=False,
                          if_exists="replace")
            if type == "redshift":
                wr.redshift.to_sql(
                    df=df,
                    con=conn,
                    table=table_name,
                    schema=redshift_kwargs["schema"],
                    chunksize=redshift_kwargs["chunksize"],
                    mode=redshift_kwargs["mode"]
                )

            logging.info("Created table %d of %d" % (i, len(load_list)))
            i += 1

        if delete_local_files:
            logging.info("Deleting local files...")
            self.delete_local_temp_files()

def handler(event, context):

    if event["log_terminal"]:

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
               # logging.FileHandler("../debug.log"),
                logging.StreamHandler()
            ]
        )
        logging.basicConfig(
            level=logging.ERROR,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
               # logging.FileHandler("../debug.log"),
                logging.StreamHandler()
            ]
        )
    else:
        logging.getLogger().setLevel(logging.INFO)

    logging.info("Starting...")
    config = event
    logging.info("%s as configs " % config)

    wl = WebLoader(**config["class"])
    logging.info("Class created with %s" % config["class"])
    wl.create_raw_files(**config["create_file"])
    logging.info("Files created with %s" % config["create_file"])
    wl.load_db(**config["load_db"])


