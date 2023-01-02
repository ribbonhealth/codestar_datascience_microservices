FROM public.ecr.aws/lambda/python:3.7


COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install torch==1.7.1+cpu torchvision==0.8.2+cpu torchaudio==0.7.2 -f https://download.pytorch.org/whl/torch_stable.html

COPY . ./
COPY app.py ./

CMD ["app.handler"]
