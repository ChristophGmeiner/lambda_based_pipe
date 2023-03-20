#!/bin/bash

rm lambda01.zip

zip -r lambda01.zip lambda/*.py

aws s3 rm s3://christophprivat-general-data-bucket/lambda01/ --recursive
aws s3 cp lambda01.zip s3://christophprivat-general-data-bucket/lambda01/

aws s3 ls s3://christophprivat-general-data-bucket/lambda01/

aws lambda update-function-code \
  --function-name web-s3-db-lambda \
  --s3-bucket christophprivat-general-data-bucket \
  --s3-key lambda01/lambda01.zip