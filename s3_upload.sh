#!/bin/bash

rm lambda01.zip
rm *.json
echo "Deleted old file!"

message1="New JSON file created"
python3 yaml_to_json.py lambda/eea_redshift.yml lambda/eea_redshift.json && echo $message1 || exit

message2="New ZIP file created!"
zip -r lambda01.zip lambda/*.py && echo $message2 || exit

message3="ZIP file moved to S3!"
aws s3 rm s3://christophprivat-general-data-bucket/lambda01/ --recursive
aws s3 cp lambda01.zip s3://christophprivat-general-data-bucket/lambda01/ && echo $message3 || exit


aws s3 ls s3://christophprivat-general-data-bucket/lambda01/

message4="Lambda Code updated!"
aws lambda update-function-code \
  --function-name web-s3-db-lambda \
  --s3-bucket christophprivat-general-data-bucket \
  --s3-key lambda01/lambda01.zip > s3_upload.json
echo $message4