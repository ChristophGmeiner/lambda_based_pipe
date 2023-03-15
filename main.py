import boto3
from urllib import request
import shutil
import os
import logging
from datetime import date
from pathlib import Path

import pandas as pd


class web_loader():
    '''
    loads data from web, stores file in S3, transfers to RDS or Redshifs
    '''

    def __init__(self,
                 file_dest_name,
                 bucket,
                 bucket_dest_folder,
                 tempfolder="/tempload/",
                 zip_file=False,
                 log_to_terminal=True):
        if tempfolder:
            if tempfolder[0] != "/":
                tempfolder = "/" + tempfolder
            if tempfolder[-1] != "/":
                tempfolder += "/"
            tempfolder = str(Path.home()) + tempfolder
            self.tempdir = tempfolder
        else:
            self.tempdir = os.curdir + "/"
        self.file_dest_name = file_dest_name
        self.bucket = bucket
        self.bucket_dest_folder = bucket_dest_folder
        if self.bucket_dest_folder[-1] != "/":
            self.bucket_dest_folder += "/"
        self.zip_file = zip_file
        self.s3_client = boto3.client("s3")
        self.date_folder = date.today().strftime("%Y%m%d") + "/"

        if log_to_terminal:
            #logging config
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(message)s",
                handlers=[
                    logging.FileHandler("debug.log"),
                    logging.StreamHandler()
                ]
            )

        logging.info("Tmp dir: %s" % self.tempdir)

    def create_raw_files(self,
                         file_download=None,
                         file_data_url=None,
                         file_format=None,
                         store_files=True):

        if file_download:
            if self.zip_file:
                dest = self.tempdir + self.file_dest_name
                if store_files:
                    r = request.urlretrieve(file_data_url, self.file_dest_name + ".zip")
                    shutil.unpack_archive(self.file_dest_name + ".zip", dest)
                    logging.info("%s unpacked to %s" % (self.file_dest_name, dest))
            else:
                if not file_format:
                    raise Exception("Parameter file_format must be provided if not zip load!")
                dest = self.tempdir + self.file_dest_name + file_format
                if store_files:
                    os.mkdir(self.file_dest_name)
                    shutil.copy(self.file_dest_name, dest)
                    logging.info("%s moved to %s" % (self.file_dest_name, dest))

        return dest


    def list_bucket_files(self,
                          store_to_local_temp=False):
        bf_list = list()
        bucket_dest_folder = self.bucket_dest_folder + self.date_folder
        bucket_files = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=bucket_dest_folder
        )
        for bf in bucket_files["Contents"]:
            bf_list.append(bf["Key"])

        if store_to_local_temp:
            source_folder = self.create_raw_files(file_download=True, store_files=False)
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

    def list_load_files(self, bucket=False):
        if bucket:
            load_list = self.list_bucket_files()

        else:
            source_folder = self.create_raw_files(file_download=True, store_files=False)
            if os.path.isdir(source_folder):
                load_list = os.listdir(source_folder)
                load_list = [source_folder + f for f in load_list]
            else:
                logging.warning("Local folder is empty ... switching to bucket!")
                load_list = self.list_bucket_files()

        return load_list


    def delete_local_temp_files(self):
        source_folder = self.create_raw_files(file_download=True, store_files=False)
        shutil.rmtree(source_folder)
        logging.info("Succesfully emptied %s!" % source_folder)
        logging.info("Tempdir now: %s" % os.listdir(self.tempdir))


    def move_raw_files_s3(self,
                          delete_local_files=True,
                          file_prefix=None,
                          file_suffix=None):
        bucket_dest_folder = self.bucket_dest_folder + self.date_folder
        source_folder = self.create_raw_files(file_download=True, store_files=False)
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



    def load_db(self,
                file_format,
                files_from_bucket=False,
                type="rds",
                delete_local_files=True):

        load_list = self.list_load_files(bucket=files_from_bucket)

        for f in load_list:
            if f[f.find(".") + 1, :] != file_format:
                raise Exception("File extensions in bucket do not match file_format parameter: %s" % load_list)
            if file_format == "csv":
                df = pd.read_csv()

        if delete_local_files:
            self.delete_local_temp_files()