import sys
import json
import pandas as pd
import numpy as np
import psycopg2
import requests
import xgboost as xgb
import os,itertools, re, pickle
from sklearn.ensemble import RandomForestClassifier
import time
from sqlalchemy import create_engine
# from utils.dbutils import (
#     norm_db_reader_conn
# )


def norm_db_reader_conn():
    """Norm cluster reader endpoint connection

    Returns:
        sqlalchemy.engine.base.Engine: [description]
    """

    DB_NORMALIZED_USER = "hw_api_master"
    DB_NORMALIZED_HOST = "healthwiz-normalized-db-cluster.cluster-ro-clexxn8nq8ps.us-west-2.rds.amazonaws.com"
    DB_NORMALIZED_NAME = "healthwiz_normalized_database_name"
    DB_NORMALIZED_PASSWORD = "3FsSaVS2kNheNYkDS5W4e6qB3eDghGcy"

    login = "postgresql://{0}:{1}@{2}:5432/{3}".format(DB_NORMALIZED_USER, DB_NORMALIZED_PASSWORD,
                                                       DB_NORMALIZED_HOST, DB_NORMALIZED_NAME)

    return create_engine(login)




def find_ngrams(text: str, number: int=3) -> set:
    """
    returns a set of ngrams for the given string
    :param text: the string to find ngrams for
    :param number: the length the ngrams should be. defaults to 3 (trigrams)
    :return: set of ngram strings
    """

    if not text:
        return set()

    words = [f'  {x} ' for x in re.split(r'\W+', text.lower()) if x.strip()]

    ngrams = set()

    for word in words:
        for x in range(0, len(word) - number + 1):
            ngrams.add(word[x:x+number])

    return ngrams


def similarity(text1: str, text2: str, number: int=3) -> float:
    """
    Finds the similarity between 2 strings using ngrams.
    0 being completely different strings, and 1 being equal strings
    """
    if text1 is None or text2 is None:
        return None
    
    ngrams1 = find_ngrams(text1, number)
    ngrams2 = find_ngrams(text2, number)

    num_unique = len(ngrams1 | ngrams2)
    num_equal = len(ngrams1 & ngrams2)
    return float(num_equal) / float(num_unique)


def token_overlap(s1: str, s2: str) -> float:
    t1 = s1.lower().replace('  ',' ').replace('   ',' ').split(' ')
    t2 = s2.lower().replace('  ',' ').replace('   ',' ').split(' ')
    return len([t for t in t1 if t1 in t2])/len(t1)


def i_token_max_similarity(s1: str, s2: str, n: int) -> float:
    t1 = s1.lower().replace('  ',' ').replace('   ',' ').split(' ')
    if n >= len(t1):
        return 0.0
    else:
        return max([similarity(t1[n], t) for t in s2.lower().replace('  ',' ').replace('   ',' ').split(' ')])


def field_similarity(s1,s2):
    return similarity(s1.lower(), s2.lower()) if s1 is not None and s2 is not None else 0


def field_token_overlap(s1,s2):
    return token_overlap(s1.lower(), s2.lower()) if s1 is not None and s2 is not None else 0


def containment(q, s):
    return int(q.lower() in s.lower()) if q is not None and s is not None else 0
    

def insurance_mapping_features(r):
    features = {
        "input_association_similarity": field_similarity(r.input_insurance, r.carrier_association),
        "input_association_to": field_token_overlap(r.input_insurance, r.carrier_association),
        "input_brand_similarity": field_similarity(r.input_insurance, r.carrier_brand),
        "input_brand_to": field_token_overlap(r.input_insurance, r.carrier_brand),
        "input_carrier_similarity": field_similarity(r.input_insurance, r.carrier_name),
        "input_carrier_to": field_token_overlap(r.input_insurance, r.carrier_name),
        "input_display_similarity": field_similarity(r.input_insurance, r.display_name),
        "input_display_to": field_token_overlap(r.input_insurance, r.display_name),
        "plan_type_in_input": containment(r.plan_type, r.input_insurance),
        "network_association_similarity": field_similarity(r.network_name, r.carrier_association),
        "network_association_to": field_token_overlap(r.network_name, r.carrier_association),
        "network_brand_similarity": field_similarity(r.network_name, r.carrier_brand),
        "network_brand_to": field_token_overlap(r.network_name, r.carrier_brand),
        "network_carrier_similarity": field_similarity(r.network_name, r.carrier_name),
        "network_carrier_to": field_token_overlap(r.network_name, r.carrier_name),
        "network_display_similarity": field_similarity(r.network_name, r.display_name),
        "network_display_to": field_token_overlap(r.network_name, r.display_name),
        "plan_type_in_network": containment(r.plan_type, r.network_name),
        "input_payer_association_similarity": field_similarity(r.input_payer_name, r.carrier_association),
        "input_payer_association_to": field_token_overlap(r.input_payer_name, r.carrier_association),
        "input_payer_carrier_similarity": field_similarity(r.input_payer_name, r.carrier_name),
        "input_payer_carrier_to": field_token_overlap(r.input_payer_name, r.carrier_name) 
    }
    return features

insurance_cols = ['input_association_similarity',
                  'input_association_to',
                  'input_brand_similarity',
                  'input_brand_to',
                  'input_carrier_similarity',
                  'input_carrier_to',
                  'input_display_similarity',
                  'input_display_to',
                  'plan_type_in_input',
                  'network_association_similarity',
                  'network_association_to',
                  'network_brand_similarity',
                  'network_brand_to',
                  'network_carrier_similarity',
                  'network_carrier_to',
                  'network_display_similarity',
                  'network_display_to',
                  'plan_type_in_network',
                  'input_payer_association_similarity',
                  'input_payer_association_to',
                  'input_payer_carrier_similarity',
                  'input_payer_carrier_to']

# code for loading model...
# model = xgb.Booster()  # init model
# model.load_model('xgbInsuranceMappingV1_02-07-2021.model')
with open('RFInsuranceMappingV2_2021_05_17.pkl','rb') as f:
    model = pickle.load(f) 

def handler(event, context):
    query_plan = json.loads(event['body'])
    
    if 'plan_name' not in query_plan:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}
    
    input_insurance = query_plan['plan_name']
    network_name = query_plan['network_name'] if 'network_name' in query_plan else None
    payer_name = query_plan['payer_name'] if 'payer_name' in query_plan else None
    
    plans_query = f"""select uuid::text
                            , carrier_association
                            , carrier_brand
                            , carrier_name
                            , insurance_keys.plan_name
                            , plan_type
                            , display_name
                    from insurance_keys where similarity(lower('{input_insurance}'), lower(display_name)) > 0.15 
                                        or similarity(lower('{input_insurance}'), lower(carrier_name)) > 0.15
                                        or similarity(lower('{input_insurance}'), lower(carrier_brand)) > 0.15"""

    if payer_name is not None:
        plans_query += f""" or similarity(lower('{payer_name}'), lower(carrier_name)) > 0.15"""
        
    norm = norm_db_reader_conn()
    ribbon_plans = pd.read_sql(plans_query, norm)
    print(ribbon_plans.shape)
    norm.dispose()
    
    ribbon_plans['network_name'] = network_name
    ribbon_plans['input_insurance'] = input_insurance
    ribbon_plans['input_payer_name'] = payer_name
    
    insurance_features = pd.DataFrame([insurance_mapping_features(r) for i,r in ribbon_plans.iterrows()])
    # score_matrix = xgb.DMatrix(insurance_features[insurance_cols].values, feature_names=insurance_cols)
    # ribbon_plans['mapping_score'] = model.predict(score_matrix)
    score_matrix = np.nan_to_num(insurance_features[insurance_cols].values)
    ribbon_plans['mapping_score'] = model.predict_proba(score_matrix)[0,1]
    ribbon_plans_scored = ribbon_plans.sort_values('mapping_score', ascending=False)
    
    
    return {'statusCode': 200,
            'body': json.dumps(ribbon_plans_scored.head(3).to_dict(orient='records')),
            'headers': {'Content-Type': 'application/json'}}
    # return 'Hello from AWS Lambda using Python' + sys.version + '!'     
