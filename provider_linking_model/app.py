import pandas as pd
import json
import pickle
from pydantic import BaseModel, ValidationError
from typing import Optional, List, Dict
import uuid as uuid_pkg

filename = 'Classifiers/gbLinkModel_V5_3_0_202303071012.model'
link_model = pickle.load(open(filename, 'rb'))


class ModelInput(BaseModel):
    """Input into provider linking model."""
    ratings_count: Optional[int] = None
    ratings_avg: Optional[float] = None
    sources: Optional[List[str]] = []
    phone_sources: Optional[List[str]] = []
    address_sources: Optional[List[str]] = []
    provider_node_uuid: Optional[uuid_pkg.UUID] = None
    location_cpli: Optional[int] = None
    phone_number: Optional[str] = None


class ModelOutputFeatures(BaseModel):
    """Model output features schema."""
    provider_node_uuid: Optional[uuid_pkg.UUID] = None
    location_cpli: Optional[int] = None
    phone_number: str
    model_version: str
    model_score: float
    confidence_score: int


class ModelOutput(BaseModel):
    """Model output schema."""

    phone_scores: List[ModelOutputFeatures]


def get_provider_linking_score(inputs: List[ModelInput]):
    """
    Return output with all input features as well as model score and confidence bin.

    :param inputs: list of model inputs for the model
    :return: output, model score and confidence bin
    """
    # create dataframe from input
    df_features = pd.DataFrame([input.dict() for input in inputs])


    df_features['source_count'] = df_features['sources'].str.len()
    df_features['address_source_count'] = df_features['address_sources'].str.len()
    df_features['phone_source_count'] = df_features['phone_sources'].str.len()

    link_output = df_features
    link_output['model_score'] = link_model.predict_proba(df_features)[:, 1]
    link_output['model_version'] = '5.3.0'
    link_output['notes'] = "Linking Model V5.3.0 for Precompute compiled on March 6, 2022."

    # create confidence scores
    thresh = [0, 0.4, 0.6, 0.8, 1.01]
    labels = [1, 2, 3, 4]
    link_output['confidence_score'] = pd.cut(link_output['model_score'], bins=thresh, labels=labels)

    link_output_dict = link_output.to_dict(orient='records')
    link_output_objects = [ModelOutputFeatures(**row) for row in link_output_dict]

    model_output = ModelOutput(phone_scores=link_output_objects).json()
    return model_output


def handler(event, context):
    try:
        data = json.loads(event["body"])
        inputs = data['features']
        output = get_provider_linking_score([ModelInput(**row) for row in inputs])
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

# test_data = """{
#     "features":
# [{
#             "ratings_count": 5,
#             "ratings_avg": 3.2,
#             "phone_number": "123",
#             "sources": ["3", "7"],
#             "phone_sources": ["3", "7"],
#             "address_sources": ["3", "7"],
#             "provider_node_uuid": "a8098c1a-f86e-11da-bd1a-00112444be1e",
#             "location_cpli": 12345
#             },
#             {
#             "ratings_count": 5,
#             "ratings_avg": 3.2,
#             "phone_number": "345",
#             "sources": ["3", "2"],
#             "phone_sources": ["3", "7"],
#             "address_sources": ["3", "7", "1"],
#             "provider_node_uuid": "a8098c1a-f86e-11da-bd1a-00112444be1e",
#             "location_cpli": 12345
# }]}
# """
# resp = handler({"body": test_data}, None)
# print(resp)
