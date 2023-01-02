import sys
import json
import pandas as pd

import requests
import numpy as np

import os,itertools, re, boto3

import time
import pickle
from polyfuzz import PolyFuzz
from polyfuzz.models import RapidFuzz
import traceback
import boto3

DATASCIENCE_MICROSERVICES_BUCKET = "rh-datascience-microservices"


def get_s3_obj(bucket, path):
    print("in get s3 obj")
    client = boto3.client('s3')
    obj = client.get_object(
        Bucket=bucket,
        Key=path,
    )
    print("after get")
    return obj['Body']


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
    if num_unique == 0:
        return 0
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


def fuzz_sim(s1, s2):
    rapidfuzz_matcher = RapidFuzz(n_jobs=1)
    pfmodel = PolyFuzz(rapidfuzz_matcher).match([s1], [s2])
    return pfmodel.get_matches().iloc[0]['Similarity']


def compute_roland_score(possible_name, cleaned_name):
    word_set_1 = possible_name.split(' ')
    word_set_2 = cleaned_name.split(' ')
    total_words = len(word_set_1) + len(word_set_2)
    count_of_syncs = 0
    for word_temp in word_set_1:
        if word_temp in word_set_2:
            count_of_syncs += 1
    for word_temp in word_set_2:
        if word_temp in word_set_1:
            count_of_syncs += 1
    roland_score = int(round(100.0 * count_of_syncs / total_words, 0))
    return roland_score


def meta_data_overlap(m1, m2):
    if m1 is None or m2 is None:
        return 0
    return float(len([m for m in m1 if m in m2]) + len([m for m in m2 if m in m1])) / (len(m1) + len(m2))


def entity_formation_features(s1: str, s2: str, o1=None, o2=None) -> dict:
    features =  {
        'overall_sim': similarity(s1, s2),
        'overall_fuzz_sim': fuzz_sim(s1, s2),
        'overall_roland': compute_roland_score(s1, s2),
        'token_overlap_1': token_overlap(s1, s2),
        'token_overlap_2': token_overlap(s2, s1),
        's1_1_token_sim': i_token_max_similarity(s1, s2, 0),
        's1_2_token_sim': i_token_max_similarity(s1, s2, 1),
        's1_3_token_sim': i_token_max_similarity(s1, s2, 2),
        's1_4_token_sim': i_token_max_similarity(s1, s2, 3),
        's1_5_token_sim': i_token_max_similarity(s1, s2, 4),
        's2_1_token_sim': i_token_max_similarity(s2, s1, 0),
        's2_2_token_sim': i_token_max_similarity(s2, s1, 1),
        's2_3_token_sim': i_token_max_similarity(s2, s1, 2),
        's2_4_token_sim': i_token_max_similarity(s2, s1, 3),
        's2_5_token_sim': i_token_max_similarity(s2, s1, 4),
        'org_npi_overlap': meta_data_overlap(o1.get('org_npis') if o1 is not None else None,
                                             o2.get('org_npis') if o2 is not None else None),
        'location_types_overlap': meta_data_overlap(o1.get('location_types') if o1 is not None else None,
                                                    o2.get('location_types') if o2 is not None else None),
        'phone_numbers_overlap': meta_data_overlap(o1.get('phone_numbers') if o1 is not None else None,
                                                   o2.get('phone_numbers') if o2 is not None else None)
    }
    return features


def compare_eval_records(model, s1: str, s2: str, model_columns: list, rm=None, em=None) -> float:
    try:
        if s1 == s2:
            return 0.999
        x = np.nan_to_num(pd.DataFrame([entity_formation_features(s1,s2, o1=rm, o2=em)])[model_columns].values)
        return model.predict_proba(x)[0,1]
    except Exception as e:
        traceback.print_exc()
        print(e, s1,s2)


def compare_record_entity(model, cols: list, record: str, entity: list, record_metadata=None, entity_metadata=None, truthset_records=None) -> dict:
    entity = [entity] if isinstance(entity, str) else entity
    comparisons = []
    if not truthset_records:
        truthset_records = []
    for entity_record in entity:
        temp_record = record
        for ts_record in truthset_records:
            for term in ts_record.split(' '):
                if term in temp_record and term in entity_record:
                    temp_record = temp_record.replace(term, '').replace('  ', ' ').strip()
                    entity_record = entity_record.replace(term, '').replace('  ', ' ').strip()
        if temp_record == '' and record != '':
            comparisons.append(1.0)
        else:
            comparisons.append(compare_eval_records(model, temp_record, entity_record, cols, rm=record_metadata, em=entity_metadata))

    if truthset_records:
        comparisons.extend(compare_record_entity(model, cols, record, entity, record_metadata, entity_metadata)['all_comparisons'])
    # comparisons = [compare_eval_records(model, record, entity_record, cols, rm=record_metadata, em=entity_metadata) for entity_record in entity]
    return {
        'mean_score': np.mean(comparisons),
        'max_score': np.max(comparisons),
        'all_comparisons': comparisons
    }


ef_feature_columns = ['overall_sim',
                      'overall_fuzz_sim',
                      'overall_roland',
                      'token_overlap_1',
                      'token_overlap_2',
                      's1_1_token_sim',
                      's1_2_token_sim',
                      's1_3_token_sim',
                      's1_4_token_sim',
                      's1_5_token_sim',
                      's2_1_token_sim',
                      's2_2_token_sim',
                      's2_3_token_sim',
                      's2_4_token_sim',
                      's2_5_token_sim',
                      'org_npi_overlap',
                      'location_types_overlap',
                      'phone_numbers_overlap']


obj = get_s3_obj(DATASCIENCE_MICROSERVICES_BUCKET, "artifacts/entity_formation_string_comparison/string_comparison_RF_v2_2021_06_30_sim_threshold.pkl")
entity_formation_rf_model = pickle.loads(obj.read())


def handler(event, context):
    data = json.loads(event['body'])

    if 'comparison_type' not in data:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}
    elif data['comparison_type'] == 'string_to_string':
        return {'statusCode': 200,
            'body': json.dumps({'model_score': compare_eval_records(entity_formation_rf_model, data['s1'], data['s2'], ef_feature_columns)}),
            'headers': {'Content-Type': 'application/json'}}
    elif data['comparison_type'] == 'string_to_entity':
        return {'statusCode': 200,
            'body': json.dumps(compare_record_entity(entity_formation_rf_model,
                                              ef_feature_columns, data['record'],
                                              data['entity'],
                                              record_metadata=data['record_metadata'] if 'record_metadata' in data else None,
                                              entity_metadata=data['entity_metadata'] if 'entity_metadata' in data else None,
                                              truthset_records=data.get('truthset_records', []))),
            'headers': {'Content-Type': 'application/json'}}
    elif data['comparison_type'] == 'entity_to_entity':
        return {'statusCode': 200,
            'body': json.dumps(compare_eval_records(entity_formation_rf_model, data['entity_1'], data['entity_2'],
                                                    ef_feature_columns, rm=data['entity_1_metadata'], em=data['entity_2_metadata'])),
            'headers': {'Content-Type': 'application/json'}}
    else:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}


# conn = norm_db_reader_conn()
# q = """select hot.id, hot.ribbon_entity_name, hot.candidate_entity
# from hcev3_objects_test hot
# where hot.id in (445102,464556)"""
# df = pd.read_sql(q, conn)
# entity_1 = df.iloc[0].to_dict()
# entity_2 = df.iloc[1].to_dict()
# entity_1['candidate_entity']['org_npis'] = entity_1['candidate_entity']['organization_npis']
# entity_2['candidate_entity']['org_npis'] = entity_2['candidate_entity']['organization_npis']
# data = {'comparison_type': 'entity_to_entity', 'entity_1': entity_1['ribbon_entity_name'], 'entity_2': entity_2['ribbon_entity_name'],
#         'entity_1_metadata': entity_1['candidate_entity'], 'entity_2_metadata': entity_2['candidate_entity']}
# data = {
# 	"comparison_type": "entity_to_entity",
# 	"entity_1": "Saint Luke's East Hospital - inpatient",
# 	"entity_2": "St Luke's East Hospital - outpatient",
# 	"entity_1_metadata": {
# 		"phone_numbers": [
# 			"8163475000"
# 		],
# 		"location_types": [
# 			"Hospital"
# 		],
# 		"org_npis": [
# 			"1093263717",
# 			"1053353490",
# 			"1467449405",
# 			"1487714879",
# 			"1114220399",
# 			"1942378351"
# 		]
# 	},
# 	"entity_2_metadata": {
# 		"phone_numbers": [
# 			"8163475000",
# 			"9999999999",
# 			"9133939729",
# 			"8163474970",
# 			"8164047500",
# 			"9136474100"
# 		],
# 		"location_types": [
# 			"Imaging Center",
# 			"Hospital"
# 		]
# 	}
# }
# event = {'body': json.dumps(data)}
# resp = handler(event, None)
# print(resp)


# if __name__ == '__main__':
#     data = {
#         "comparison_type": "string_to_entity",
#         "record": "upenn",
#         "entity": "upenn neurology department",
#         "entity_metadata": {
#             "phone_numbers": ["8585789600"]
#         },
#         "record_metadata": {
#             "phone_numbers": ["8584579000", "8585789600"]
#         }
#     }
#     event = {'body': json.dumps(data)}
#     resp = handler(event, None)
#     print(resp)
