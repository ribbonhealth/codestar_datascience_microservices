import pandas as pd
from twilio.rest import Client
import json
import re
from sqlalchemy import create_engine
from datetime import datetime
import boto3
import ast
import os

SECRETS_PREFIX = "dev-lambda-"
SECRETS_NORM = f"{SECRETS_PREFIX}norm"
SECRETS_TWILIO = f"{SECRETS_PREFIX}twilio"


def get_norm_secrets():
    return (
        os.environ['DB_NORMALIZED_USER'],
        os.environ['DB_NORMALIZED_PASSWORD'],
        os.environ['DB_NORMALIZED_HOST_RW'],
        os.environ['DB_NORMALIZED_NAME'],
    )


def get_twilio_secrets():
    return os.environ["TWILIO_ACCT_SID"], os.environ["TWILIO_AUTH_TOKEN"],


def clean_phones(phone_number):
    # remove non-numberic characters
    clean_number = re.sub('[^0-9]', '', phone_number)

    # return last 10 digits of a 11 digit number
    if len(clean_number) == 11:
        return clean_number[1:]
    else:
        return clean_number


def norm_connection():
    login = "postgresql://{0}:{1}@{2}:5432/{3}".format(*get_norm_secrets())
    return create_engine(login)

def check_existing_phones(phone_number, conn):
    q = "SELECT * FROM twilio_phones WHERE phone = {}".format(phone_number)

    twilio_phones = pd.read_sql(q, conn)

    if len(twilio_phones.index) == 0:
        return False, {}
    else:
        return True, twilio_phones['lookup_response'][0]


def append_new_phone(twilio_response, engine):
    twilio_response['created_at'] = datetime.now()
    twilio_response.to_sql("twilio_phones", engine, if_exists='append', index=False)


def get_twilio_lookup_data(phone_number, engine):
    """
    returns Twilio "carrier" lookup call response for given phone number

    :param phonenumber: phone number to look up in Twilio carrier API
    :return: 'had_exception' boolean flag, and Twilio API response if no exception
        ('phone', 'mobile_country_code', 'mobile_network_code', 'name', 'type',
        'error_code')
    """

    account_sid, auth_token = get_twilio_secrets()

    resp = {'phone': phone_number}
    try:
        client = Client(account_sid, auth_token)
        phone_number_lookup = client.lookups \
            .phone_numbers(phone_number) \
            .fetch(type=['carrier'])
        for k in phone_number_lookup.carrier.keys():
            resp[k] = phone_number_lookup.carrier[k]
        resp['had_exception'] = False

        twilio_response = pd.DataFrame([[phone_number, json.dumps(resp)]], columns=['phone', 'lookup_response'])
        append_new_phone(twilio_response, engine)
        return resp
    except Exception as e:
        print(e)
        resp['had_exception'] = True

        twilio_response = pd.DataFrame([[phone_number, json.dumps(resp)]], columns=['phone', 'lookup_response'])
        append_new_phone(twilio_response, engine)
        return resp


def handler(event, context):
    data = json.loads(event['body'])

    if 'phone' not in data:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}
    else:
        phone = clean_phones(data['phone'])

        engine = norm_connection()

        status, twilio_response = check_existing_phones(phone, engine)

        if status == True:
            return {'statusCode': 200,
                    'body': json.dumps(twilio_response),
                    'headers': {'Content-Type': 'application/json'}}
        else:
            return {'statusCode': 200,
                    'body': json.dumps(get_twilio_lookup_data(phone, engine)),
                    'headers': {'Content-Type': 'application/json'}}
