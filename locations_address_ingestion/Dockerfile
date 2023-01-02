FROM public.ecr.aws/lambda/python:3.7


COPY locations_address_ingestion/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY locations_address_ingestion/ ./locations_address_ingestion/
COPY utils/ ./utils/

CMD ["locations_address_ingestion.app.handler"]
