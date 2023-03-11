FROM public.ecr.aws/lambda/python:3.9


COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./
COPY app.py ./

CMD ["app.handler"]
#CMD ["app.py"]
#ENTRYPOINT ["python3"]