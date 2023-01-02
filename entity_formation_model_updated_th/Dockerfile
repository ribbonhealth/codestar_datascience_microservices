FROM public.ecr.aws/lambda/python:3.7


COPY entity_formation_model_updated_th/requirements.txt ./
RUN yum install gcc -y
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install torch==1.7.1+cpu torchvision==0.8.2+cpu torchaudio==0.7.2 -f https://download.pytorch.org/whl/torch_stable.html

COPY entity_formation_model_updated_th/ ./entity_formation_model_updated_th/
COPY utils/ ./utils/

CMD ["entity_formation_model_updated_th.app.handler"]

