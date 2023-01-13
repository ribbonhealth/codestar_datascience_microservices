from __future__ import print_function
import os
import json
import re
import joblib
import numpy as np
import pandas as pd
from locations_resolver import parse_address_components
import boto3
import io

DATASCIENCE_MICROSERVICES_BUCKET = "rh-ds-microservices"


def get_s3_obj(bucket, path):
    print("in get s3 obj")
    client = boto3.client('s3')
    obj = client.get_object(
        Bucket=bucket,
        Key=path,
    )
    print("after get")
    return obj['Body']


obj = get_s3_obj(DATASCIENCE_MICROSERVICES_BUCKET, "artifacts/hce_confidence/RFLocationCorrectnessV4_5-6-2022.model")
correctness_model = joblib.load(io.BytesIO(obj.read()))
obj = get_s3_obj(DATASCIENCE_MICROSERVICES_BUCKET, "artifacts/hce_confidence/RFPhoneCorrectnessV4_5-6-2022.model")
phone_correctness_model = joblib.load(io.BytesIO(obj.read()))
model_path = 'models'
with open(os.path.join(model_path, 'RFLocationCorrectnessV4_5-6-2022_cols.json'), 'rb') as inp:
    correctness_cols = json.load(inp)
with open(os.path.join(model_path, 'RFPhoneCorrectnessV4_5-6-2022_cols.json'), 'rb') as inp:
    phone_correctness_cols = json.load(inp)

class ScoringService(object):
    model = None  # Where we keep the model when it's loaded
    correctness_model = None
    correctness_cols = None
    phone_model = None
    phone_cols = None

    @classmethod
    def parse_input_data(cls, input_data, cols):
        input_data['google_findplace_address_components'] = parse_address_components(input_data['elements']['google_place_result']) if 'google_place_result' in \
                                                                                                                                       input_data[
                                                                                                                                           'elements'] else None
        if input_data['google_findplace_address_components'] is not None:
            input_data['street_equal'] = int(
                input_data['address_components']['street_number'] == input_data['google_findplace_address_components']['street_number'])
            input_data['city_equal'] = int(input_data['address_components']['city'] == input_data['google_findplace_address_components']['city'])
            input_data['state_equal'] = int(input_data['address_components']['state'] == input_data['google_findplace_address_components']['state'])
            input_data['zip_equal'] = int(input_data['address_components']['zip'] == input_data['google_findplace_address_components']['zip'])
        else:
            input_data['street_equal'] = 0
            input_data['city_equal'] = 0
            input_data['state_equal'] = 0
            input_data['zip_equal'] = 0

        input_data['source_count'] = len(input_data['elements']['sources'])
        input_data['similarity'] = input_data['elements']['google_chosen_name_similarity'] or 0.0 if 'google_chosen_name_similarity' in input_data[
            'elements'] else None
        input_data['phone_count'] = len(input_data['elements']['raw_phone_numbers'])
        input_data['distinct_phone_count'] = len(input_data['elements']['phone_numbers'])

        input_data['permanently_closed_flag'] = int(input_data['elements']['permanently_closed_flag']) if 'permanently_closed_flag' in input_data[
            'elements'] else 0
        input_data['rating'] = int('rating' in input_data['elements']['google_place_result']) if 'google_place_result' in input_data['elements'] else 0
        input_data['photo_count'] = len(input_data['elements']['google_place_result']['photos']) if 'google_place_result' in input_data['elements'] and 'photos' in input_data['elements']['google_place_result'] else 0
        input_data['reviews_count'] = len(input_data['elements']['google_place_result']['reviews']) if 'google_place_result' in input_data['elements'] and 'reviews' in input_data['elements']['google_place_result'] else 0
        input_data['google_name_isnull'] = int(input_data['elements']['google_name_cleansed'] == '')
        input_data['street_equal_isnull'] = int(input_data['google_findplace_address_components'] is None)

        for source_col in cols['categorical']['sources']:
            input_data[source_col] = int(source_col[10:] in input_data['elements']['sources'])

        for gtype_col in cols['categorical']['google_types']:
            input_data[gtype_col] = int(gtype_col[9:] in input_data['elements']['google_types'] if 'google_types' in input_data['elements'] else 0)

        return input_data

    @classmethod
    def predict_location_correctness(cls, input_data):
        parsed_data = pd.DataFrame([cls.parse_input_data(input_data, correctness_cols)])
        x = np.nan_to_num(parsed_data[correctness_cols['covar_cols']].values)
        return correctness_model.predict_proba(x)[0, 1]

    @classmethod
    def predict_phone_confidences(cls, input_data):
        str_regex = re.compile('[^0-9]')
        parsed_data = cls.parse_input_data(input_data, phone_correctness_cols)
        phone_scores = []
        for phone in parsed_data['elements']['phone_numbers']:
            parsed_data['specific_phone_count'] = len([p for p in parsed_data['elements']['raw_phone_numbers'] if p == phone])
            parsed_data['findplace_phone_scrubbed'] = str_regex.sub('', parsed_data['elements']['google_place_result'][
                'formatted_phone_number']) if 'google_place_result' in parsed_data['elements'] and 'formatted_phone_number' in parsed_data['elements'][
                'google_place_result'] else None
            parsed_data['google_phone_same'] = int(phone == parsed_data['findplace_phone_scrubbed'])

            parsed_data_df = pd.DataFrame(parsed_data)
            x = np.nan_to_num(parsed_data_df[phone_correctness_cols['phone_covar_cols']].values)
            phone_scores.append({'phone': phone, 'score': phone_correctness_model.predict_proba(x)[0, 1]})
        return phone_scores


def handler(event, context):

    data = json.loads(event['body'])
    result = ScoringService.predict_location_correctness(data)
    confidence_bin = None
    if result >= 0.79:
        confidence_bin = 4
    elif result >= 0.62:
        confidence_bin = 3
    elif result >= 0.25:
        confidence_bin = 2
    else:
        confidence_bin = 1
    phone_scores = ScoringService.predict_phone_confidences(data)
    phone_scores.sort(key=lambda x: x['score'], reverse=True)
    resp = json.dumps({'location_correctness_score': result,
                                               'location_correctness_score_bin': confidence_bin,
                                               'phone_scores': phone_scores})
    return {'statusCode': 200,
            'body': resp,
            'headers': {'Content-Type': 'application/json'}}

# input = '{"address_components": {"subpremise": {"STE": "100", "formatted_subpremise": "# 100"}, "street_number": "44", "intersection": null, "route": "Route 23 North", "city": "Riverdale", "state": "NJ", "zip": "07457", "country": "US", "is_tokenized": true, "formatted_address": "44 Route 23 North # 100, Riverdale, NJ 07457, US"}, "resolve_method": "score_location", "elements": {"google_place_result": {"address_components": [{"long_name": "STE 100", "short_name": "STE 100", "types": ["subpremise"]}, {"long_name": "Riverdale", "short_name": "Riverdale", "types": ["locality", "political"]}, {"long_name": "Morris County", "short_name": "Morris County", "types": ["administrative_area_level_2", "political"]}, {"long_name": "New Jersey", "short_name": "NJ", "types": ["administrative_area_level_1", "political"]}, {"long_name": "United States", "short_name": "US", "types": ["country", "political"]}, {"long_name": "07457", "short_name": "07457", "types": ["postal_code"]}], "adr_address": "44 Route 23 North, <span class=\\"street-address\\">STE 100</span>, <span class=\\"locality\\">Riverdale</span>, <span class=\\"region\\">NJ</span> <span class=\\"postal-code\\">07457</span>, <span class=\\"country-name\\">USA</span>", "business_status": "OPERATIONAL", "formatted_address": "44 Route 23 North, STE 100, Riverdale, NJ 07457, USA", "formatted_phone_number": "(973) 839-5004", "geometry": {"location": {"lat": 40.98598399999999, "lng": -74.30381539999999}, "viewport": {"northeast": {"lat": 40.98727093029149, "lng": -74.3023801697085}, "southwest": {"lat": 40.98457296970849, "lng": -74.3050781302915}}}, "icon": "https://maps.gstatic.com/mapfiles/place_api/icons/generic_business-71.png", "id": "f8545be1b106b202086f87e01ea507b8ad7940f1", "international_phone_number": "+1 973-839-5004", "name": "Progressive Diagnostic Imaging", "opening_hours": {"open_now": false, "periods": [{"close": {"day": 1, "time": "1800"}, "open": {"day": 1, "time": "0800"}}, {"close": {"day": 2, "time": "1800"}, "open": {"day": 2, "time": "0800"}}, {"close": {"day": 3, "time": "2000"}, "open": {"day": 3, "time": "0800"}}, {"close": {"day": 4, "time": "1800"}, "open": {"day": 4, "time": "0800"}}, {"close": {"day": 5, "time": "1800"}, "open": {"day": 5, "time": "0800"}}, {"close": {"day": 6, "time": "1300"}, "open": {"day": 6, "time": "0800"}}], "weekday_text": ["Monday: 8:00 AM \\u2013 6:00 PM", "Tuesday: 8:00 AM \\u2013 6:00 PM", "Wednesday: 8:00 AM \\u2013 8:00 PM", "Thursday: 8:00 AM \\u2013 6:00 PM", "Friday: 8:00 AM \\u2013 6:00 PM", "Saturday: 8:00 AM \\u2013 1:00 PM", "Sunday: Closed"]}, "photos": [{"height": 501, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAuNfn2W_T-bv1THyNE_suEN1Z2W7iCvDnMqj0SksdnzZxTFpYtomo1CtpzM6PDrZOlreKbqacwDaeKsFwoq0k-jH-WESOQ9WaFQyMtsNPN2p6wX6cfm7C2g3SbNW5heNqEhDSORm9iKRDbNOT996fgEMmGhQju4jH7ejS9336MP3djBIWXqCnUA", "width": 891}, {"height": 1266, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAOvnUF0bNZJiJvDlj81DwoLRYYnDoUlbJLkR7ZLswM__hi5epWOHZJQoLAfAgdh2aE1nE-e_naxdJfpATBGYltGc9MHRLD4BAEaLfhYHNjj3FMbpJY8n9-MMjF2HY_EisEhDYwA_w95pHAbpKtImKxekbGhStMCOOq9fDL4SXNaxAUz3HpAXPGA", "width": 1900}, {"height": 536, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAiO3Brr4ZHX044kJsKtMDNATLqFQquzTN42BmORrZHPAodyHdlSBDnrQioKkP2BXfNYOWcDgGn4EzAXz6M_8UwBk-y45rY7dtFinBIWyP6bP8Li1X4WJ8re1-URszBG2vEhCQbvbijd5gVkIe4CMKKvoKGhQzogBTKi9vn68FiF9ZSkoMgMCIqA", "width": 768}, {"height": 1080, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAi_H_29j8lQrinkP-WKBRupn4Gvud-j2xo_Mnew-Kwx3YyG_rG8yAozHA0SwwU7MpznlWkWcj_DkGProo-vE6Je5O_GFPSqxs3fWJ4Rk4HqB1qWLZtMOMpA7OWtzLzD9aEhAsPsLSDEf-Oyg_f2wz3mcPGhRqrGjtrJDpa__G8kN8mrhx7I7aUA", "width": 1620}, {"height": 1080, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAqZq9Mo5Ez3UZt0MgV2y4X9uED4WiwVIGYqVjVP2GvZuplynScZ-5ad_BXgcS0WNuosDHtHFfv7N9ck6YWOQR3_5R9uTSsAmpn0sF7mCpNrs1SjFTe2f0JVSf-lLBvQTYEhCaPoRF5YLBU6ZaxZjzZ8KLGhRk6mGyw9rBP2hUfP4DKa2uUfF_uA", "width": 1620}, {"height": 500, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAA36dzg-9flmUlddW1wyMLao6m3YHSXgFTwtgKQ77-Bvg38aOqvC2bTpA8_yUn08ACFkv-xfeEb5aXDHc0AEhzHzOxF6ml8llpM59I0fZTYO-4RRz34ZajaUuVjKMzyP8XEhC0kIHuCYazdn9Y6neds74qGhQahl4cZgpGL66DlLa71Fg97GlcAw", "width": 500}, {"height": 858, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAzFP-E4cGp0oAmUl7xHsLlHdDMJk41arKU8I2JxdUEDb-kz9LjYH3PJp27LfMrxQENNOjIser2S_K6ZFII54ViFLuq5RJj-AIB9utQ9Ueh3zZw_An4zQavrcM3Vmwd3c1EhB0lsT48ssr3r7zX36_k_KhGhQayDEfXpAIQ27E3XGV8x0wEODXwg", "width": 576}, {"height": 1080, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAABxGYw-G8LzntVgnPrtot8XIBYdotaIKTiwPFxrmDW9jlJ6r8WQ2kNAGR1w2m05YzkxoIirwOb4Cyj39vJWgQtA5vWOs9G4gMh2_4RwU1SE2C9TEVe-xngfTDO8W5LX7OEhDqeTi1Zk3c9rSz-8ZJ-anpGhS4sYQhXrj8k2cRvIXRcvYB-pjDAQ", "width": 1620}, {"height": 1080, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAWAm-FVt9XegMzg_T5hFAn6vot_ahj34VydMhbVmDj4pjZiJlGB7r_STS1Q_3YWngVLWH8mMQFi3oQpgTyAecC9D9vuv76S_58yC0B5_EaRwkk5rlw8dzDydmcgtybjFpEhAMFgQhk-JvuOwhyUR177kWGhRNJuol6cyeBQ6Qbmz1hyy3-oXBTg", "width": 1620}, {"height": 795, "html_attributions": ["<a href=\\"https://maps.google.com/maps/contrib/117081247698325110645\\">Progressive Diagnostic Imaging</a>"], "photo_reference": "CmRaAAAAmZhxxd-XZ-C-CD0baExUf6AW34RzVEafLgbL-SqCa3U5OCrbglIYLt2l7NcgRoYLI1IZYave3fTDygj4l5vHxQDkuC0CiIdmX77LJzohZFzu3k9p0-O7rS8jY40srMdMEhAuJtA0VBmErU8c4gE-ByY8GhTyMDJRbFZR3Ae78YAqD-OXYA2Tfw", "width": 796}], "place_id": "ChIJK0dCT1oDw4kRaGlpbLzgAu0", "plus_code": {"compound_code": "XMPW+9F Riverdale, NJ, United States", "global_code": "87G7XMPW+9F"}, "rating": 4.8, "reference": "ChIJK0dCT1oDw4kRaGlpbLzgAu0", "reviews": [{"author_name": "Taylor Kishfy", "author_url": "https://www.google.com/maps/contrib/110327253474618781185/reviews", "language": "en", "profile_photo_url": "https://lh5.ggpht.com/-c_Z1ypZlI6M/AAAAAAAAAAI/AAAAAAAAAAA/xzIGZheH4Kc/s128-c0x00000000-cc-rp-mo/photo.jpg", "rating": 5, "relative_time_description": "6 months ago", "text": "They do a great job. I was in and out faster than I planned. Excellent facility and very friendly staff. The owner Nick was extremely receptive and had actually helped me schedule my appointment himself! The best diagnostic imaging facilities I have been to. Highly Recommended", "time": 1574452138}, {"author_name": "Sarah dinicola", "author_url": "https://www.google.com/maps/contrib/106117389817726065238/reviews", "language": "en", "profile_photo_url": "https://lh5.ggpht.com/-CkOSXWsircA/AAAAAAAAAAI/AAAAAAAAAAA/vf4XXFVS6ug/s128-c0x00000000-cc-rp-mo/photo.jpg", "rating": 5, "relative_time_description": "3 months ago", "text": "At a time when most needed, Progressive Diagnostic Imaging in Riverdale provided outstanding service from start to finish.  Beginning with the ease of making an appointment, their comforting staff, state of the art equipment and fast results - I could not have asked for a better experience.  Thank you.", "time": 1582919512}, {"author_name": "Karen Heenan", "author_url": "https://www.google.com/maps/contrib/104645786820121803365/reviews", "language": "en", "profile_photo_url": "https://lh3.ggpht.com/-Hw9NJteLy08/AAAAAAAAAAI/AAAAAAAAAAA/6KHR2gaRnc4/s128-c0x00000000-cc-rp-mo/photo.jpg", "rating": 5, "relative_time_description": "6 months ago", "text": "I had a great experience when  having an MRI of my Brain, w and w/o contrast.  I am very claustrophobic and the tech put me at ease and let me know she was always there and supporting me.  Always asked how I was, very kind and thoughtful.   I have recommended Progressive to a friend of mine and will continue to use them.   Office staff was also very helpful and with scheduling and paperwork. .  Appreciated it.\\nThanks,  Karen Heenan.", "time": 1575051969}, {"author_name": "ryan seide", "author_url": "https://www.google.com/maps/contrib/110124618983443028163/reviews", "language": "en", "profile_photo_url": "https://lh3.ggpht.com/-mTNLbtjGIz4/AAAAAAAAAAI/AAAAAAAAAAA/mIwgQIWfH3g/s128-c0x00000000-cc-rp-mo/photo.jpg", "rating": 5, "relative_time_description": "6 months ago", "text": "I had the most pleasant experience with my technician, Chic. I had come in for a MRI with and without contrast. I suffer from restless leg syndrome, and couldn\\u2019t stay still very well. She was patient and kind even though my legs weren\\u2019t helping matters. I had to go back a second time for the image with contrast. She was so gentle giving me the IV I never felt it. I have to bring my Mother in for a test and will be requesting her and recommending her to friends and family.   Kathy Seide", "time": 1574815769}, {"author_name": "Tom H", "author_url": "https://www.google.com/maps/contrib/107262614295333588614/reviews", "language": "en", "profile_photo_url": "https://lh6.ggpht.com/-g6HXNtzKZmY/AAAAAAAAAAI/AAAAAAAAAAA/2Sa8gi1_nfI/s128-c0x00000000-cc-rp-mo/photo.jpg", "rating": 5, "relative_time_description": "6 months ago", "text": "Went today for an MRI, had an appointment but they were running a bit behind. Offered me water or coffee while I waited which was nice.  Place is clean, front desk and technicians were amazing, very friendly and they seem to have new machines. Overall a great visit, would use them going forward for any scans I need", "time": 1576276917}], "scope": "GOOGLE", "types": ["health", "point_of_interest", "establishment"], "url": "https://maps.google.com/?cid=17078459836819663208", "user_ratings_total": 63, "utc_offset": -240, "vicinity": "44 Route 23 North, STE 100, Riverdale", "website": "https://pdirad.com/"}, "sources": ["4350", "19", "https://d3ul0st9g52g6o.cloudfront.net/ffm/cms-data-index.json", "https://doctorfinder.horizonblue.com/cms-data/json/index.json", "14", "0", "16", "357", "10", "654", "68", "853", "33", "12", "3723", "166", "83", "91"], "phone_numbers": ["8883405850", "9738395004", "9735712121", "8442899963", "8778002450", "8008722875"], "raw_phone_numbers": ["8883405850", "9738395004", "9735712121", "8442899963", "8778002450", "8008722875"], "google_name_cleansed": "Progressive Diagnostic Imaging", "google_chosen_name_similarity": 1.0, "permanently_closed_flag": false, "google_types": ["health", "point_of_interest", "establishment"]}}'
# resp = handler({'body': input}, None)
# print(resp)