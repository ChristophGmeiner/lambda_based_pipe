#!/bin/bash

aws s3 cp *.yml s3://christophprivat-general-data-bucket/lambda01/
aws s3 cp main.py s3://christophprivat-general-data-bucket/lambda01/

aws s3 ls s3://christophprivat-general-data-bucket/lambda01/