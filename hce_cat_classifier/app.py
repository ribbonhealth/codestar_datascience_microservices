import nltk
from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.tree import Tree
import json
import pickle
from fuzzywuzzy import fuzz
import pandas as pd
from utils.db_utils import (
    norm_db_conn,
 )
import importlib.resources
import hce_cat_classifier
from datetime import datetime

# nltk.data.path.append("/tmp")
# nltk.download('punkt', download_dir = "/tmp")
# nltk.download('averaged_perceptron_tagger', download_dir = "/tmp")
# nltk.download('maxent_ne_chunker', download_dir = "/tmp")
# nltk.download('words', download_dir = "/tmp")


def categorize_entity(entity_name, google_place_data, log_result = False):
    conn, _ = norm_db_conn()

    rbn_name = entity_name.lower()
    upper_name = rbn_name.upper()
    token_count = len(nltk.word_tokenize(rbn_name))

    nltk_labels = [tok.label() for tok in ne_chunk(pos_tag(word_tokenize(upper_name))) if type(tok) == Tree]
    person_count = len([i for i in nltk_labels if i == 'PERSON'])
    org_count = len([i for i in nltk_labels if i == 'ORGANIZATION'])
    gpe_count = len([i for i in nltk_labels if i == 'GPE'])
    location_count = len([i for i in nltk_labels if i == 'LOCATION'])

    hours = 'opening_hours' in google_place_data
    google_phone = 'formatted_phone_number' in google_place_data

    business_status = google_place_data.get('business_status')
    name = google_place_data.get('name')
    types = google_place_data.get('types', [])
    user_ratings_total = google_place_data.get('user_ratings', 0)

    google_similarity = fuzz.ratio(rbn_name, name)

    X = pd.DataFrame({'rbn_name': [rbn_name], 'token_count': [token_count], 'types': [types], 'business_status': [business_status],
                      'hours': [hours], 'google_phone': [google_phone], 'user_ratings_total': [user_ratings_total],
                      'person_count': [person_count], 'org_count': [org_count], 'gpe_count': [gpe_count], 'location_count': [location_count],
                      'google_similarity': [google_similarity]})

    model_version = 'RF_pipe_v0.pkl'
    with importlib.resources.open_binary(hce_cat_classifier, model_version) as f:
        clf = pickle.load(f)

    y = clf.predict(X)
    y_prob = clf.predict_proba(X)
    if log_result:
        pd.DataFrame([{
            'model_version': model_version,
            'name': rbn_name,
            'place_id': google_place_data.get('place_id'),
            'google_place_data': json.dumps(google_place_data),
            'location_category': y[0],
            'location_category_score':y_prob[0][1],
        }]).to_sql('location_category_classification_lookup', conn, method='multi', index=False, if_exists='append')
    return {'location_category': y[0], 'location_category_score': y_prob[0][1]}


def handler(event, context):
    data = json.loads(event['body'])
    if 'entity_name' not in data or 'google_place_data' not in data:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}
    else:
        return {'statusCode': 200,
                'body': json.dumps(categorize_entity(data['entity_name'], data['google_place_data'], data.get('log_result', False))),
                'headers': {'Content-Type': 'application/json'}}


# if __name__ == "__main__":
#     google_place_data = {'address_components': [{'long_name': '6621', 'short_name': '6621', 'types': ['street_number']}, {'long_name': 'West Maple Road', 'short_name': 'W Maple Rd', 'types': ['route']}, {'long_name': 'West Bloomfield Township', 'short_name': 'West Bloomfield Township', 'types': ['locality', 'political']}, {'long_name': 'Oakland County', 'short_name': 'Oakland County', 'types': ['administrative_area_level_2', 'political']}, {'long_name': 'Michigan', 'short_name': 'MI', 'types': ['administrative_area_level_1', 'political']}, {'long_name': 'United States', 'short_name': 'US', 'types': ['country', 'political']}, {'long_name': '48322', 'short_name': '48322', 'types': ['postal_code']}, {'long_name': '3004', 'short_name': '3004', 'types': ['postal_code_suffix']}], 'adr_address': '<span class="street-address">6621 W Maple Rd</span>, <span class="locality">West Bloomfield Township</span>, <span class="region">MI</span> <span class="postal-code">48322-3004</span>, <span class="country-name">USA</span>', 'business_status': 'OPERATIONAL', 'formatted_address': '6621 W Maple Rd, West Bloomfield Township, MI 48322, USA', 'formatted_phone_number': '(248) 254-1208', 'geometry': {'location': {'lat': 42.5410466, 'lng': -83.40256579999999}, 'viewport': {'northeast': {'lat': 42.5423968302915, 'lng': -83.40115416970848}, 'southwest': {'lat': 42.5396988697085, 'lng': -83.4038521302915}}}, 'icon': 'https://maps.gstatic.com/mapfiles/place_api/icons/v1/png_71/generic_business-71.png', 'icon_background_color': '#7B9EB0', 'icon_mask_base_uri': 'https://maps.gstatic.com/mapfiles/place_api/icons/v2/generic_pinlet', 'international_phone_number': '+1 248-254-1208', 'name': 'Rehab Institute of Michigan', 'place_id': 'ChIJ2yuNosG6JIgRNiSKBq5yapM', 'plus_code': {'compound_code': 'GHRW+CX West Bloomfield Township, MI, USA', 'global_code': '86JRGHRW+CX'}, 'rating': 5, 'reference': 'ChIJ2yuNosG6JIgRNiSKBq5yapM', 'reviews': [{'author_name': 'Jay Z', 'author_url': 'https://www.google.com/maps/contrib/105385038778168706520/reviews', 'profile_photo_url': 'https://lh3.googleusercontent.com/a-/ACNPEu-dTz1abbLjRGK-Iqq_sOGqfm8KhRYFYrPH0-up=s128-c0x00000000-cc-rp-mo-ba4', 'rating': 5, 'relative_time_description': '3 years ago', 'text': '', 'time': 1543873686, 'translated': False}], 'types': ['physiotherapist', 'health', 'point_of_interest', 'establishment'], 'url': 'https://maps.google.com/?cid=10622428762866328630', 'user_ratings_total': 1, 'utc_offset': -240, 'vicinity': '6621 West Maple Road, West Bloomfield Township'}
#     event = {'body': json.dumps({'entity_name': 'Rehabilitation Institute PHY', 'google_place_data': google_place_data})}

