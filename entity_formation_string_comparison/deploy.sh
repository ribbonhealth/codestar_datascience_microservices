DATE_WITH_TIME=`date "+%Y%m%d%H%M%S"`

TAG=$1
TAG=${TAG:=latest}

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 741242253094.dkr.ecr.us-west-2.amazonaws.com
docker build -t datascience/entity_formation_string_comparison_model .

docker tag datascience/entity_formation_string_comparison_model:latest 741242253094.dkr.ecr.us-west-2.amazonaws.com/datascience/entity_formation_string_comparison_model:$TAG
docker push 741242253094.dkr.ecr.us-west-2.amazonaws.com/datascience/entity_formation_string_comparison_model:$TAG
