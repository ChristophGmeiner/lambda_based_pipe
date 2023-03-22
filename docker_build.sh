docker build -t web_s3_db_lambda .

docker run -web_s3_db_lambda

aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 120327452865.dkr.ecr.eu-central-1.amazonaws.com/web_s3_db_lambda

docker images

docker tag web_s3_db_lambda 120327452865.dkr.ecr.eu-central-1.amazonaws.com/web_s3_db_lambda:latest

docker push 120327452865.dkr.ecr.eu-central-1.amazonaws.com/web_s3_db_lambda:latest

#test

docker run -p 9004:8080  web_s3_db_lambda:latest

curl -XPOST "http://localhost:9004/2015-03-31/functions/function/invocations" -d '{}'

aws lambda update-function-code \
      --function-name web-s3-db-docker-lambda \
      --image-uri 120327452865.dkr.ecr.eu-central-1.amazonaws.com/web_s3_db_lambda:latest02