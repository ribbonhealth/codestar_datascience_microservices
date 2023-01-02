import json
import random
from pydantic import BaseModel, ValidationError
from typing import List
import uuid as uuid_pkg


class PhoneInput(BaseModel):
    """Phone number schema."""
    phone_number: str
    sources: List[int]


class ModelInput(BaseModel):
    """Model input schema."""
    provider_node_uuid: uuid_pkg.UUID
    location_node_uuid: uuid_pkg.UUID
    sources: List[int]
    phone_numbers: List[PhoneInput]


class PhoneScore(BaseModel):
    """Phone score schema."""
    phone_number: str
    model_score: float
    confidence_score: int
    model_version: str


class ModelOutput(BaseModel):
    """Model output schema."""
    provider_node_uuid: uuid_pkg.UUID
    location_node_uuid: uuid_pkg.UUID
    model_score: float
    confidence_score: int
    model_version: str
    phone_scores: List[PhoneScore]


def mock_score(input: ModelInput) -> ModelOutput:
    """Function that mocks the scoring function for providers - service locations links."""
    provider_node_uuid = input.provider_node_uuid
    location_node_uuid = input.location_node_uuid
    sources = input.sources
    phone_numbers = [{"phone_number": phone.phone_number, "sources": phone.sources} for phone in input.phone_numbers]

    return ModelOutput(
        provider_node_uuid=provider_node_uuid,
        location_node_uuid=location_node_uuid,
        model_score=0.5,
        confidence_score=random.randint(1, 5),
        model_version="0.1",
        phone_scores=[PhoneScore(phone_number=phone["phone_number"],
                                 model_score=0.5,
                                 confidence_score=random.randint(1, 5),
                                 model_version="0.1") for phone in phone_numbers]
    )


def handler(event, context):
    """Handler for lambda."""
    try:
        input = ModelInput.parse_raw(event["body"])
        output = mock_score(input)
        return {'statusCode': 200,
                'body': output.json(),
                'headers': {'Content-Type': 'application/json'}}

    except ValidationError as e:
        return {'statusCode': 400,
                'body': json.dumps(str(e)),
                'headers': {'Content-Type': 'application/json'}}

    except Exception as e:
        return {'statusCode': 500,
                'body': json.dumps(str(e)),
                'headers': {'Content-Type': 'application/json'}}

