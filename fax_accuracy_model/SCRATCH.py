#%%
import pandas as pd
import json
import pickle
import sys
import os

# #%%
# sys.path.extend(['/Users/aschwartz/Projects/DataScience-Microservice/fax_accuracy_model'])
# print('Python %s on %s' % (sys.version, sys.platform))
#
# #%%
# print(sys.path)
#
# #%%
# print(os.path.realpath('__file__'))
# print(os.getcwd())
# os.chdir('/Users/aschwartz/Projects')
# print(os.getcwd())

#%%
notebook_path = os.path.abspath("DataScience-Microservice/fax_accuracy_model/SCRATCH.py")
print(os.path.dirname(notebook_path))
filename = os.path.join(os.path.dirname(notebook_path), "Classifiers/fax_model_fewer_features.model")

#%%
model = pickle.load(open(filename, 'rb'))

#%%

body = {'npi': '123',
        'address': '1234',
        'fax_number': '6106131817',
        'source_count': 6,
        'num_locations_with_this_fax': 2,
        'num_npis_with_this_fax': 1,
        'num_fax_for_this_location': 6,
        'fax_sources' = {0},
        'is_doc' = 0}

df_body = pd.DataFrame(body)

numeric_features = ['source_count', 'num_locations_with_this_fax', 'num_npis_with_this_fax',
                    'num_fax_for_this_location']
categorical_array_features = ['fax_sources']
categorical_features = []
boolean_features = ['is_doc']

features = numeric_features + categorical_array_features + categorical_features + boolean_features

data_to_score = df_body[features]

#%%
output = copy.deepcopy(data_to_score)
output['model_score'] = model.predict_proba(data_to_score)[:,1]
output['model_predicted_class'] = model.predict(data_to_score)
output['model_version'] = mod_type
output['notes'] = "Fax model v1 completion"


data = json.loads(event['body'])
