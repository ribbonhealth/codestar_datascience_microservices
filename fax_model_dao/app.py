import pandas as pd
import json
import pickle
import copy

def get_fax_score(f):
    """
    returns output with all the input features as well as a model score and model predicted class

    :param f: dict of data for the model (features + uuids [npi, address, fax])
    :return: output + 'model_score' and 'model_predicted_class'
    """

    df_features = pd.DataFrame(f)

    filename = 'Classifiers/gbt_fax_v2_202210061454.model'
    model = pickle.load(open(filename, 'rb'))

    output = []

    numeric_features = ['source_count']
    categorical_array_features = ['fax_sources']
    categorical_features = []
    boolean_features = ['is_doc']

    features = numeric_features + categorical_array_features + categorical_features + boolean_features

    data_to_score = df_features[features]

    output = copy.deepcopy(df_features)
    output['model_score'] = model.predict_proba(data_to_score)[:,1]
    output['model_predicted_class'] = model.predict(data_to_score)

    return output

def check_input_format(f):
    """
    checks if the features are formatted correctly
    uses just the first item if the input is a list

    :param f: feature dict with the following columns:
        - npi: string
        - address: int64
        - fax_number: string
        - source_count: int64
        - num_locations_with_this_fax: int64
        - num_npis_with_this_fax: int64
        - num_fax_for_this_location: int64
        - fax_sources: list of int64
        - is_doc: int64
    :return: boolean flag if input is formatted correctly
    """

    f_0 = f[0] if isinstance(f, list) else f

    dict_ideal_format = {'npi': '1234',
            'address': 125554,
            'fax_number': '6106133331817',
            'source_count': 6,
            'fax_sources': [1],
            'is_doc': 0}
    df_ideal_format = pd.DataFrame(dict_ideal_format)
    dtypes_ideal_format = dict(df_ideal_format.dtypes)

    try:
        df_f = pd.DataFrame(f_0)
        dtypes_f = dict(df_f.dtypes)
        return dtypes_f == dtypes_ideal_format
    except Exception as e:
        return False

def handler(event, context):
    data = json.loads(event['body'])
    features = data['features']

    if check_input_format(features):
        return {'statusCode': 200,
                'body': get_fax_score(features).to_json(),
                'headers': {'Content-Type': 'application/json'}}
    else:
        return {'statusCode': 400,
                'body': 'error',
                'headers': {'Content-Type': 'application/json'}}

# test_data = '''{
# 	"features": [
# 		{
# 			"npi": "1234",
# 			"address": 125554,
# 			"fax_number": "6106133331817",
# 			"source_count": 6,
# 			"fax_sources": [
# 				1
# 			],
# 			"is_doc": 0
# 		}
# 	]
# }'''
#
# resp = handler({'body': test_data}, None)
# print(resp)