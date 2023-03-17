#!/bin/bash

zip -r lambda01.zip lambda01

aws s3 rm s3://christophprivat-general-data-bucket/lambda01/ --recursive
aws s3 cp lambda01.zip s3://christophprivat-general-data-bucket/lambda01/

aws s3 ls s3://christophprivat-general-data-bucket/lambda01/