DATE_WITH_TIME=`date "+%Y%m%d%H%M%S"`

TAG=$1
TAG=${TAG:=latest}

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 404889086824.dkr.ecr.us-west-2.amazonaws.com
docker build -t datascience/provider_location_confidence .

docker tag datascience/provider_location_confidence:latest 404889086824.dkr.ecr.us-west-2.amazonaws.com/datascience/provider_location_confidence:$TAG
docker push 404889086824.dkr.ecr.us-west-2.amazonaws.com/datascience/provider_location_confidence:$TAG