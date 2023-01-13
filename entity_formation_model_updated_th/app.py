import json
import pandas as pd
import numpy as np
import itertools, re
import pickle
from polyfuzz import PolyFuzz
from polyfuzz.models import RapidFuzz
import traceback
from py_stringmatching.similarity_measure import affine, bag_distance, generalized_jaccard
from utils.parsing_utils import EntityStringParsing,no_meaningful_diff
from utils.string_parser_lookup import departments_lookup, medical_entities, field_roots
from utils.geographic_utils import is_geotag_diff, is_hospital_cpli, pull_geotags

esp = EntityStringParsing(
    medical_entities=medical_entities,
    fields=field_roots,
    departments=departments_lookup
)

import boto3

DATASCIENCE_MICROSERVICES_BUCKET = "rh-ds-microservices"


aff = affine.Affine()
bag = bag_distance.BagDistance()
gen_jac = generalized_jaccard.GeneralizedJaccard()

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

def token_overlap(s1: str, s2: str) -> float:
    t1 = s1.lower().replace('  ',' ').replace('   ',' ').split(' ')
    t2 = s2.lower().replace('  ',' ').replace('   ',' ').split(' ')
    return len([t for t in t1 if t1 in t2])/len(t1)

def fuzz_sim(s1, s2):
    rapidfuzz_matcher = RapidFuzz(n_jobs=1)
    pfmodel = PolyFuzz(rapidfuzz_matcher).match([s1], [s2])
    return pfmodel.get_matches().iloc[0]['Similarity']


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

def find_fuzzsim_string_diffs(s1, s2):
    """Finds the average string similarity for all words that are different between the two strings"""
    s1_words, s2_words = set(s1.split()), set(s2.split())
    s1_diffs = s1_words.difference(s2_words)
    s2_diffs = s2_words.difference(s1_words)
    # get the fuzz similarity between those differences (common misspellings=high, non=low)
    if s2_diffs != set():
        try:
            s1_diff_fuzz_sims = np.max(
                [fuzz_sim(i, j) for (i, j) in itertools.product(s2_diffs, s1_words)]
            )
        except Exception:
            s1_diff_fuzz_sims = 0
    else:
        s1_diff_fuzz_sims = 0

    if s1_diffs != set():
        try:
            s2_diff_fuzz_sims = np.max(
                [fuzz_sim(i, j) for (i, j) in itertools.product(s1_diffs, s2_words)]
            )
        except Exception:
            s2_diff_fuzz_sims = 0
    else:
        s2_diff_fuzz_sims = 0

    return s1_diff_fuzz_sims, s2_diff_fuzz_sims

def create_char_grams(s, n):
    s = s.replace(' ', '').replace('  ', '')
    return [s[i:i+n] for i in range(len(s)-1)]

def find_char_diffs(s1, s2, n):
    char_s1, char_s2 = set(create_char_grams(s=s1, n=n)), set(create_char_grams(s=s2, n=n))
    char_s1_diff = len(char_s1.difference(char_s2))
    char_s2_diff = len(char_s2.difference(char_s1))
    char_s1_inter = len(char_s1.intersection(char_s2))
    char_s2_inter = len(char_s2.intersection(char_s1))
    numer = (char_s1_diff + char_s2_diff)
    denom = (char_s1_inter + char_s2_inter)
    return numer / denom if denom > 0 else numer

def most_similar_words_distance(s1, s2, char_len=3):
    """Finds the two most similar words in a certain string"""
    s1 = set([j for j in [i.lower().strip() for i in s1.split()] if len(j) >= char_len])
    s2 = set([j for j in [i.lower().strip() for i in s2.split()] if len(j) >= char_len])
    diff_s1 = s1.difference(s2)
    diff_s2 = s2.difference(s1)
    prods = [*itertools.product(diff_s1, diff_s2)]
    value = 0
    if prods:
        for prod in prods:
            distance = fuzz_sim(prod[0], prod[1])
            if distance >= value:
                value = distance
        return distance # distance between two most similar words
    else:
        return 1 # exactly the same

def entity_formation_features(comp_record: dict, s1: str, s2: str, o1=None, o2=None) -> dict:
    diff_fuzz_sim = find_fuzzsim_string_diffs(s1, s2)
    features =  {
        'overall_sim': similarity(s1, s2),
        'overall_fuzz_sim': fuzz_sim(s1, s2),
        'overall_roland': compute_roland_score(s1, s2),
        'token_overlap_1': token_overlap(s1, s2),
        'token_overlap_2': token_overlap(s2, s1),
        'word_diff_fuzz_sim_s1': diff_fuzz_sim[0],
        'word_diff_fuzz_sim_s2': diff_fuzz_sim[1],
        'char_diff_fuzz_sim_bigrams': find_char_diffs(s1, s2, 2),
        'char_diff_fuzz_sim_trigrams': find_char_diffs(s1, s2, 3),
        'dept_vs_speciality_flag': comp_record['business_logic_match']['dept_vs_speciality_flag'],
        'affine_gap': aff.get_raw_score(s1, s2),
        'bag_distance': bag.get_sim_score(s1, s2),
        'gen_jaccard_sim': gen_jac.get_sim_score(s1.split(), s2.split()),
        'count_entity_differences': len(comp_record['entity_differences']),
        'count_common_speciality_entities': len(comp_record['entity_overlap']),
        'count_common_medical_entities': len(comp_record['common_medical_entities']),
        'count_diff_medical_elements': len(comp_record['diff_medical_elements']),
        'count_diff_department_elements': len(comp_record['diff_department_elements']),
        's1_1_token_sim': i_token_max_similarity(s1, s2, 0),
        's1_2_token_sim': i_token_max_similarity(s1, s2, 1),
        's1_3_token_sim': i_token_max_similarity(s1, s2, 2),
        's2_1_token_sim': i_token_max_similarity(s2, s1, 0),
        's2_2_token_sim': i_token_max_similarity(s2, s1, 1),
        's2_3_token_sim': i_token_max_similarity(s2, s1, 2),
        'org_npi_overlap': meta_data_overlap(o1.get('org_npis') if o1 is not None else None,
                                             o2.get('org_npis') if o2 is not None else None),
        'location_types_overlap': meta_data_overlap(o1.get('location_types') if o1 is not None else None,
                                                    o2.get('location_types') if o2 is not None else None),
        'phone_numbers_overlap': meta_data_overlap(o1.get('phone_numbers') if o1 is not None else None,
                                                   o2.get('phone_numbers') if o2 is not None else None),

    }
    return features




def is_diff_flag(record):
    diff_flags = [
        record['business_logic_match']['mismatched_specialty_flag'],
        record['business_logic_match']['mismatched_department_flag'],
        record['business_logic_match']['dept_vs_speciality_flag']
    ]
    return True if any(diff_flags) else False

def is_same_flag(record, s1, s2, rm, em):
    # parse out the needed information from the records
    record_cpli = rm['cpli'] if 'cpli' in rm.keys() else None
    entity_cpli = em['cpli'] if 'cpli' in em.keys() else None
    if record_cpli is not None and entity_cpli is not None:
        hospital_cplis = all([is_hospital_cpli(record_cpli), is_hospital_cpli(entity_cpli)])
        cond_one = is_geotag_diff(s1=s1, s2=s2, cpli=record_cpli)
    else:
        hospital_cplis = False
        cond_one = False

    two_med_entities = record['business_logic_match']['dual_medical_entities_flag']
    other_matches_flag = is_diff_flag(record) == False
    # first condition: both are medical entities, and have no other specialities
    cond_two = all([
        hospital_cplis, two_med_entities, other_matches_flag
    ])
    # the only difference between the two is a medical entity
    cond_three = record['business_logic_match']['medical_entities_sole_diff']
    # the only difference is a geographic difference
    # both records have all the same entity, and have string sim > .90
    cond_four = record['business_logic_match']['entity_overlap_string_sim']
    # one string is fully encompassed in the other; and the difference has no additional entities
    cond_five = no_meaningful_diff(s1, s2, record) if hospital_cplis else False
    # if any of the conditions above are true; mark as the same entity
    same_entity = any([cond_one, cond_two, cond_three, cond_four, cond_five])
    return True if same_entity else False

def remove_geotags(string, cpli):
    geotags = pull_geotags(cpli)
    for geotag in geotags:
        if geotag in string:
            string = string.replace(geotag, '')
    return string

def compare_eval_records(model, s1: str, s2: str, model_columns: list,  rm=None, em=None) -> float:
    s1, s2 = s1.lower(), s2.lower()
    try:
        if s1 == s2:
            return 0.999

        record = esp.create_comparison_records(string_one=s1, string_two=s2)
        same_flag = is_same_flag(record=record, s1=s1, s2=s2, rm=rm, em=em)
        diff_flag = is_diff_flag(record=record)
        record_cpli = rm['cpli'] if 'cpli' in rm.keys() else None
        entity_cpli = em['cpli'] if 'cpli' in em.keys() else None
        if record_cpli is not None:
            s1 = remove_geotags(s1, record_cpli)
        if entity_cpli is not None:
            s2 = remove_geotags(s2, entity_cpli)

        # check the various buisness rules
        if diff_flag:
            return 0.001
        elif same_flag:
            return 0.999
        else:
            X = np.nan_to_num(
                    pd.DataFrame(
                        [entity_formation_features(comp_record=record, s1=s1, s2=s2, o1=rm, o2=em)]
                    )[model_columns].values
            )
            return model.predict_proba(X)[0, 1]
    except Exception as e:
        traceback.print_exc()
        print(e, s1,s2)


def compare_record_entity(model, cols: list, record: str, entity: list, record_metadata=None, entity_metadata=None, truthset_records=None) -> dict:
    entity = [entity] if isinstance(entity, str) else entity
    comparisons = []
    for entity_record in entity:
        eval_record = compare_eval_records(
            model=model,
            s1=record,
            s2=entity_record,
            model_columns=EF_FEATURE_COLUMNS,
            rm=record_metadata,
            em=entity_metadata
        )
        comparisons.append(eval_record)

    if truthset_records:
        comparisons.extend(compare_record_entity(model, cols, record, entity, record_metadata, entity_metadata)['all_comparisons'])
    # comparisons = [compare_eval_records(model, record, entity_record, cols, rm=record_metadata, em=entity_metadata) for entity_record in entity]
    return {
        'mean_score': np.mean(comparisons),
        'max_score': np.max(comparisons),
        'all_comparisons': comparisons
    }

obj = get_s3_obj(DATASCIENCE_MICROSERVICES_BUCKET, "artifacts/entity_formation_string_comparison/entity_resolution_model_2022_09_30_updated_featset.pkl")
entity_formation_rf_model = pickle.loads(obj.read())
# make sure the features are in the correct order
EF_FEATURE_COLUMNS = entity_formation_rf_model.feature_names_in_

def handler(event, context):
    data = json.loads(event['body'])
    if 'comparison_type' not in data:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}
    elif data['comparison_type'] == 'string_to_string':
        return {
            'statusCode': 200,
            'body': json.dumps({'model_score': compare_eval_records(entity_formation_rf_model, data['s1'], data['s2'], EF_FEATURE_COLUMNS)}),
            'headers': {'Content-Type': 'application/json'}}
    elif data['comparison_type'] == 'string_to_entity':
        return {
            'statusCode': 200,
            'body': json.dumps(
            compare_record_entity(
                entity_formation_rf_model,
                EF_FEATURE_COLUMNS,
                data['record'],
                data['entity'],
                record_metadata=data['record_metadata'] if 'record_metadata' in data else None,
                entity_metadata=data['entity_metadata'] if 'entity_metadata' in data else None,
                truthset_records=data.get('truthset_records', [])
            )),
            'headers': {'Content-Type': 'application/json'}}
    elif data['comparison_type'] == 'entity_to_entity':
        return {'statusCode': 200,
            'body': json.dumps(compare_eval_records(entity_formation_rf_model, data['entity_1'], data['entity_2'],
                                                    EF_FEATURE_COLUMNS, rm=data['entity_1_metadata'], em=data['entity_2_metadata'])),
            'headers': {'Content-Type': 'application/json'}}
    else:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}

# from utils.db_utils import norm_db_reader_conn
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

# if __name__ == '__main__':
#     event = {'body': json.dumps(data)}
#     resp = handler(event, None)
#     print(resp)
#
# event = {'body': json.dumps(data)}
# resp = handler(event, None)
# print(resp)

# {'statusCode': 200, 'body': '{"mean_score": 0.0984766405301673, "max_score": 0.1041387488896441, "all_comparisons": [0.09281453217069051, 0.1041387488896441]}', 'headers': {'Content-Type': 'application/json'}}
if __name__ == '__main__':
    data = {'comparison_type': 'string_to_entity',
            'record': 'Cleveland Clinic',
            'entity': ['Cleveland Clinic', 'Cleveland Clinic - Cardiology', 'Random String of Ohio'],
            'record_metadata': {'cpli': None, 'org_npis': None, 'phone_numbers': None, 'location_types': None},
            'entity_metadata': {'cpli': None, 'org_npis': None, 'phone_numbers': None, 'location_types': None}}
    event = {'body': json.dumps(data)}
    resp = handler(event, None)
    print(resp)