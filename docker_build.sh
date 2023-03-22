docker build -t web_s3_db_lambda .

docker run -web_s3_db_lambdadocker

aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 120327452865.dkr.ecr.eu-central-1.amazonaws.com/web_s3_db_lambda

docker images

docker tag 5b0e174ef4cd 120327452865.dkr.ecr.eu-central-1.amazonaws.com/web_s3_db_lambda:web_s3_db_lambda

docker push 120327452865.dkr.ecr.eu-central-1.amazonaws.com/web_s3_db_lambda:web_s3_db_lambda