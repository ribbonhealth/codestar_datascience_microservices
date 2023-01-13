DATE_WITH_TIME=`date "+%Y%m%d%H%M%S"`

TAG=$1
TAG=${TAG:=latest}

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 404889086824.dkr.ecr.us-west-2.amazonaws.com
docker build -t datascience/fax_accuracy_model .

docker tag datascience/fax_accuracy_model:latest 404889086824.dkr.ecr.us-west-2.amazonaws.com/datascience/fax_accuracy_model:$TAG
docker push 404889086824.dkr.ecr.us-west-2.amazonaws.com/datascience/fax_accuracy_model:$TAG
