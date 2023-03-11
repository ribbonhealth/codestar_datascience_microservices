import numpy as np
import pandas as pd
import json
import pickle
from pydantic import BaseModel, ValidationError
from typing import Optional, List, Dict
import uuid as uuid_pkg
from ast import literal_eval


class ModelInput(BaseModel):
    """Input into provider ranking model."""
    node_1_uuid: Optional[uuid_pkg.UUID] = None
    node_2_uuid: Optional[uuid_pkg.UUID] = None
    intermediate_node_uuid: Optional[int] = None
    provider__ratings_avg: Optional[float] = None
    provider__ratings_count: Optional[int] = None
    provider__service_location__source_ids: Optional[List[int]] = []
    provider__provider_types: Optional[List[str]] = []
    provider__specialties: Optional[List[str]] = []
    service_location__location_types: Optional[List[str]] = []
    service_location__confidence: Optional[int] = None
    service_location__location_category_score: Optional[float] = None
    provider__address__phone_numbers: Optional[List[Dict]] = []
    service_location__phone_numbers: Optional[List[Dict]] = []


class ModelOutput(BaseModel):
    """Model output features."""
    node_1_uuid: Optional[uuid_pkg.UUID] = None
    node_2_uuid: Optional[uuid_pkg.UUID] = None
    intermediate_node_uuid: Optional[int] = None
    model_version: str
    model_rank_score: float


def get_provider_ranking_score(inputs: List[ModelInput]):
    """
    Return output with all input features and model_rank_score.

    :param inputs: list of model inputs for the model
    :return: output, model probability score
    """
    # create dataframe from input
    df = pd.DataFrame([input.dict() for input in inputs])

    # df['source_prov_sl_list'] = df['provider__service_location__source_ids'].apply(
    #     lambda x: literal_eval(x) if pd.notna(x) else [])
    df['source_prov_sl_list'] = df['provider__service_location__source_ids']
    df['source_prov_sl_count'] = df['source_prov_sl_list'].apply(lambda x: len(x))
    # df['provider__provider_types'] = df['provider__provider_types'].apply(lambda x: literal_eval(x) if pd.notna(x) else [])
    df['provider__provider_types_count'] = df['provider__provider_types'].apply(lambda x: len(x))
    # df['provider__specialties'] = df['provider__specialties'].apply(lambda x: literal_eval(x) if pd.notna(x) else [])
    df['provider__specialties_count'] = df['provider__specialties'].apply(lambda x: len(x))
    # df['service_location__location_types'] = df['service_location__location_types'].apply(
    #     lambda x: literal_eval(x) if pd.notna(x) else [])
    df['service_location__location_types_count'] = df['service_location__location_types'].apply(lambda x: len(x))
    df['provider__ratings_avg'] = df['provider__ratings_avg'].apply(lambda x: x if pd.notna(x) else 0)
    df['service_location__confidence'] = df['service_location__confidence'].apply(
        lambda x: str(x) if pd.notna(x) else '0')
    # df['service_location__phone_numbers'] = df['service_location__phone_numbers'].apply(lambda x: literal_eval(x) if pd.notna(x) else [])

    #create overlap features
    df['provider_phones'] = [set([i.get('phone') for i in row]) for row in df.provider__address__phone_numbers]
    df['sl_phones'] = [set([i.get('phone') for i in row]) for row in df.service_location__phone_numbers]
    df['phone_overlap'] = np.where(df['provider_phones']&df['sl_phones'], 'TRUE', 'FALSE')

    filename = 'Classifiers/gbLinkModel_V5_2_5_202303101617.model'
    link_model = pickle.load(open(filename, 'rb'))

    link_output = df
    link_output['model_rank_score'] = link_model.predict_proba(df)[:, 1]
    link_output['model_version'] = "1.0.1"
    link_output['notes'] = "Ranking Model V1.0.1 for compiled_edges trained on Mar 9, 2023."

    link_output_dict = link_output.to_dict(orient='records')[0]
    model_output = ModelOutput(node_1_uuid=link_output_dict.get('node_1_uuid'),
                               node_2_uuid=link_output_dict.get('node_2_uuid'),
                               intermediate_node_uuid=link_output_dict.get('intermediate_node_uuid'),
                               model_version="1.0.1",
                               model_rank_score=link_output_dict.get('model_rank_score')
                               )

    return model_output


def handler(event, context):
    try:
        data = json.loads(event["body"])
        inputs = data['features']
        output = get_provider_ranking_score([ModelInput(**row) for row in inputs])
        print(output)
        return {'statusCode': 200,
                'body': output,
                'headers': {'Content-Type': 'application/json'}}
    except ValidationError as e:
        return {'statusCode': 400,
                'body': json.dumps(str(e)),
                'headers': {'Content-Type': 'application/json'}}
    except Exception as e:
        return {'statusCode': 500,
                'body': json.dumps(str(e)),
                'headers': {'Content-Type': 'application/json'}}


#
# test_data = """{
#     "features":
# [{
#             "node_1_uuid": "06998ae8-2231-4c9a-8539-20e27c9c3360",
#             "node_2_uuid": "48ebd1e7-1e54-489d-9fd6-fb547a695ef4",
#             "intermediate_node_uuid": 3761184,
#             "provider__ratings_avg": 4.3,
#             "provider__ratings_count": 3,
#             "provider__service_location__source_ids": [3, 7],
#             "provider__provider_types": ["Doctor", "Nursing"],
#             "provider__specialties": ["aff0d1a2-5fbe-41cb-a854-270c03528a43", "475b20fc-c188-4ca1-8e3a-b0614b8f8ce2"],
#             "service_location__location_types": [],
#             "service_location__confidence": 4,
#             "service_location__location_category_score": 0.6385194529609867,
#             "provider__address__phone_numbers": [{"phone": "9019427456", "details": "primary"}],
#             "service_location__phone_numbers": [{"phone": "9019427456", "score": 0.3403787878787879, "detail": "primary"}]
#
# }]}
# """
# resp = handler({"body": test_data}, None)
# print(resp)
#
