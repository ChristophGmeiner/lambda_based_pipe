#!/bin/bash

bucket_name="christophprivat-lambda-functions"
bucketpath="s3://$bucket_name"
filebasename="web_s3_db_lambda"
filename="$filebasename.zip"
pyname="$filebasename.py"

rm $filename && echo "Old ZIP deleted!"

message1="New JSON file created"
python3 yaml_to_json.py eea_redshift.yml eea_redshift.json && echo $message1 || exit

if [ $1 = "--install-deps" ]
  then
    echo "Creating packages folder..."
    pip install -q -r requirements.txt --target ./package && echo "pip finished"
    message2="New ZIP file created with package folder!"
    zip -r -q $filename package && echo $message2 || exit
fi

message2b="Py script added to ZIP file!"
zip -q $filename $pyname && echo $message2b

message3="ZIP file moved to S3!"
aws s3 cp $filename $bucketpath && echo $message3 || exit


aws s3 ls $bucketpath

if [ $2 = "--lambda-function-update" ]
  then
    message4="Lambda Code updated!"
    aws lambda update-function-code \
      --function-name web-s3-db-lambda \
      --s3-bucket $bucket_name \
      --s3-key $filename > s3_upload.json
    echo $message4
fi

