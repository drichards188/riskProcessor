rm -rf package
mkdir package
cd package
pip install -r ../req_hack.txt --target .

aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 585068124165.dkr.ecr.us-east-2.amazonaws.com

docker build -t lambda-api . -f Dockerfile.aws.lambda

docker tag lambda-api:latest 585068124165.dkr.ecr.us-east-2.amazonaws.com/lambda-api:latest

docker push 585068124165.dkr.ecr.us-east-2.amazonaws.com/lambda-api:latest
