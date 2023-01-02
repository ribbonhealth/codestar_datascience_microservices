# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/

import boto3
import pandas as pd
import psycopg2
import requests
import urllib
import uuid
from sqlalchemy import create_engine
import ast
import os

def get_boto_session():
    return boto3.Session(region_name='us-west-2')

def __pull_global_secret(varname, boto_client):
    secret_vars_dict = ast.literal_eval(boto_client.get_secret_value(SecretId=varname)['SecretString'])
    if 'value' in secret_vars_dict:
        return secret_vars_dict['value']
    raise Exception()


def get_boto_session():
    return boto3.Session(region_name='us-west-2')

def __pull_global_secret(varname, boto_client):
    secret_vars_dict = ast.literal_eval(boto_client.get_secret_value(SecretId=varname)['SecretString'])
    if 'value' in secret_vars_dict:
        return secret_vars_dict['value']
    raise Exception()

def get_env_var(varname):
    if varname in os.environ:
        val = os.environ[varname]
        return val

    secret_name = 'DS_MS_SECRETS'  # "dev-internal-tool-secrets"
    client = get_boto_session().client(service_name='secretsmanager')
    secret_vars_dict = ast.literal_eval(client.get_secret_value(SecretId=secret_name)['SecretString'])

    if varname in secret_vars_dict:
        # logger.info(f'secret_vars_dict={secret_vars_dict}')
        val = secret_vars_dict[varname]
        if not val.startswith('GLOBAL_'):
            return val
        # We need to pull the global secret then
        res_val = __pull_global_secret(val, client)
        return res_val
    else:
        return None


def norm_db_conn():
    # DB_NORMALIZED_USER = "temp_external_user"
    DB_NORMALIZED_USER = get_env_var(varname='DB_NORMALIZED_USER')
    DB_NORMALIZED_HOST = get_env_var(varname='DB_NORMALIZED_HOST')
    DB_NORMALIZED_NAME = get_env_var(varname='DB_NORMALIZED_NAME')
    DB_NORMALIZED_PASSWORD = get_env_var(varname='DB_NORMALIZED_PASSWORD')
    # DB_NORMALIZED_USER = os.environ['DB_NORMALIZED_USER']
    # DB_NORMALIZED_HOST = os.environ['DB_NORMALIZED_HOST']
    # DB_NORMALIZED_NAME = os.environ['DB_NORMALIZED_NAME']
    # DB_NORMALIZED_PASSWORD = os.environ['DB_NORMALIZED_PASSWORD']

    login = "postgresql://{0}:{1}@{2}:5432/{3}".format(DB_NORMALIZED_USER, DB_NORMALIZED_PASSWORD,
                                                       DB_NORMALIZED_HOST, DB_NORMALIZED_NAME)

    conn = psycopg2.connect("host='{0}' dbname='{1}' user='{2}' password='{3}'".format(
        DB_NORMALIZED_HOST,
        DB_NORMALIZED_NAME,
        DB_NORMALIZED_USER,
        DB_NORMALIZED_PASSWORD
    ))
    conn.autocommit = True
    return create_engine(login), conn


def norm_db_reader_conn():
    # DB_NORMALIZED_USER = "temp_external_user"
    DB_NORMALIZED_USER = get_env_var(varname='DB_NORMALIZED_USER')
    DB_NORMALIZED_HOST = get_env_var(varname='DB_NORMALIZED_HOST')
    DB_NORMALIZED_NAME = get_env_var(varname='DB_NORMALIZED_NAME')
    DB_NORMALIZED_PASSWORD = get_env_var(varname='DB_NORMALIZED_PASSWORD')
    # DB_NORMALIZED_USER = os.environ['DB_NORMALIZED_USER']
    # DB_NORMALIZED_HOST = os.environ['DB_NORMALIZED_HOST']
    # DB_NORMALIZED_NAME = os.environ['DB_NORMALIZED_NAME']
    # DB_NORMALIZED_PASSWORD = os.environ['DB_NORMALIZED_PASSWORD']

    login = "postgresql://{0}:{1}@{2}:5432/{3}".format(DB_NORMALIZED_USER, DB_NORMALIZED_PASSWORD,
                                                       DB_NORMALIZED_HOST, DB_NORMALIZED_NAME)

    return create_engine(login)


def rapid_db_conn():
    DB_NORMALIZED_USER = get_env_var(varname='DB_RAPID_USER')
    DB_NORMALIZED_HOST =  get_env_var(varname='DB_RAPID_HOST')
    DB_NORMALIZED_NAME = get_env_var(varname='DB_RAPID_NAME')
    DB_NORMALIZED_PASSWORD = get_env_var(varname='DB_RAPID_PASSWORD')
    login = "postgresql://{0}:{1}@{2}:5432/{3}".format(DB_NORMALIZED_USER, DB_NORMALIZED_PASSWORD,
                                                       DB_NORMALIZED_HOST, DB_NORMALIZED_NAME)

    conn = psycopg2.connect("host='{0}' dbname='{1}' user='{2}' password='{3}'".format(DB_NORMALIZED_HOST,
                                                                                       DB_NORMALIZED_NAME,
                                                                                       DB_NORMALIZED_USER,
                                                                                       DB_NORMALIZED_PASSWORD))
    conn.autocommit = True
    return create_engine(login), conn


