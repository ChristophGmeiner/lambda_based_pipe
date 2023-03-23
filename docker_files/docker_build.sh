#!/bin/bash

echo "$1"
echo "$2"

docker build -t $1 .

#docker run -p 9004:8080 $1

aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 120327452865.dkr.ecr.eu-central-1.amazonaws.com/"$1"

docker tag $1 120327452865.dkr.ecr.eu-central-1.amazonaws.com/"$1":latest

docker push 120327452865.dkr.ecr.eu-central-1.amazonaws.com/"$1":latest

#test

#curl -XPOST "http://localhost:9004/2015-03-31/functions/function/invocations" -d '{"class": {"file_dest_name": "eea", "tempfolder": "/tmp/", "bucket": "christophprivat-general-data-bucket", "bucket_dest_folder": "eea", "file_download": false, "file_format": "json"}, "create_file": {"store_files": true, "file_data_url": "https://discodata.eea.europa.eu/sql?query=SELECT%20*%20FROM%20CO2Emission.latest.co2cars%20WHERE%20year%20%3D%202017&p=1&nrOfHits=20000&mail=null&schema=null"}, "load_db": {"secret_name": "test-rs-serverless", "files_from_bucket": false, "type": "redshift", "redshift_kwargs": {"glue_conn": "glue-connection-test-rs-serverless", "schema": "public", "chunksize": 1000, "mode": "overwrite"}}}'

aws lambda update-function-code \
      --function-name "$2" \
      --image-uri 120327452865.dkr.ecr.eu-central-1.amazonaws.com/"$1":latest > docker_lambda_update.json