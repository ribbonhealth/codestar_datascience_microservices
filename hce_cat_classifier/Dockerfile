FROM public.ecr.aws/lambda/python:3.7


COPY hce_cat_classifier/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m nltk.downloader punkt -d /var/lang/nltk_data
RUN python -m nltk.downloader averaged_perceptron_tagger -d /var/lang/nltk_data
RUN python -m nltk.downloader maxent_ne_chunker -d /var/lang/nltk_data
RUN python -m nltk.downloader words -d /var/lang/nltk_data

COPY hce_cat_classifier/ ./hce_cat_classifier/
COPY utils/ ./utils/

# pickle file expects ColumnTransformer at the root level
COPY hce_cat_classifier/ColumnTransformers.py ./

CMD ["hce_cat_classifier.app.handler"]