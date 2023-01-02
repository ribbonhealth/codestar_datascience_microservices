import pandas as pd
import json
from entity_formation_model_updated_th.app import handler
import pytest

test_payloads = [
    {
        "comparison_type": "string_to_entity",
        "record": "advanced healthcare urology",
        "entity": [
            "advanced healthcare neurology",
        ],
        "entity_metadata": {
            "cpli" : 12345
        },
        "record_metadata": {
            "cpli" : 12345
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "advanced healthcare emergency room",
        "entity": [
            "advanced healthcare ambulatory surgical center",
        ],
        "entity_metadata": {
            "cpli": 12345
        },
        "record_metadata": {
            "cpli": 12345
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "advanced healthcare emergency room",
        "entity": [
            "advanced healthcare urology",
        ],
        "entity_metadata": {
            "cpli": 12345
        },
        "record_metadata": {
            "cpli": 12345
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "nyu departmen of nephrolog",
        "entity": [
            "nyu department of nephrology",
        ],
        "entity_metadata": {
            "cpli": 12345
        },
        "record_metadata": {
            "cpli": 12345
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "emory decatur medical center",
        "entity": [
            "emory decatur hospital",
        ],
        "entity_metadata": {
            "cpli" : 3259484
        },
        "record_metadata": {
            "cpli" : 3259484
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "emory decatur",
        "entity": [
            "emory decatur hospital",
        ],
        "entity_metadata": {
            "cpli": 3259484
        },
        "record_metadata": {
            "cpli": 3259484
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "jefferson health northeast",
        "entity": [
            "jefferson health",
        ],
        "entity_metadata": {
            "cpli": 2383383
        },
        "record_metadata": {
            "cpli": 2383383
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "Ear nose and Throat",
        "entity": [
            "Ear nose and Throat Eugene",
        ],
        "entity_metadata": {
            "cpli": 3524666
        },
        "record_metadata": {
            "cpli": 3524666
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "upenn urology",
        "entity": ["upenn nephrology"],
        "entity_metadata": {
            "phone_numbers": ["8585789600"],
            "cpli" : 3524666
        },
        "record_metadata": {
            "phone_numbers": ["8584579000", "8585789600"],
            "org_npis": ["1962597807"],
            "cpli" : 3524666
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "upenn urology",
        "entity": ["upenn nephrology"],
        "entity_metadata": {
            "phone_numbers": ["8585789600"]
        },
        "record_metadata": {
            "phone_numbers": ["8584579000", "8585789600"],
            "org_npis": ["1962597807"]
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "upenn urology",
        "entity": ["upenn nephrology"],
        "entity_metadata": {
            "phone_numbers": ["8585789600"],
            "cpli" : 3524666
        },
        "record_metadata": {
            "phone_numbers": ["8584579000", "8585789600"],
            "org_npis": ["1962597807"]
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "upenn urology",
        "entity": ["upenn nephrology"],
        "entity_metadata": {
            "phone_numbers": ["8585789600"],

        },
        "record_metadata": {
            "phone_numbers": ["8584579000", "8585789600"],
            "org_npis": ["1962597807"],
            "cpli": 3524666
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "upenn urology",
        "entity": ["upenn nephrology"],
        "entity_metadata": {
            "phone_numbers": ["8585789600"],
            "cpli": None
        },
        "record_metadata": {
            "phone_numbers": ["8584579000", "8585789600"],
            "org_npis": ["1962597807"],
            "cpli": None
        }
    },
    {
        "comparison_type": "string_to_entity",
        "record": "upenn urology",
        "entity": ["upenn nephrology"],
        "entity_metadata": {
            "phone_numbers": None,
            "cpli": None
        },
        "record_metadata": {
            "phone_numbers": None,
            "org_npis": ["1962597807"]
        }
    }

]

def test_comparison_strings(result, expected_result):
    assert result == expected_result

if __name__ == '__main__':
    test_results = [
        0.001, 0.001, 0.001, 0.999,
        0.999, 0.999, 0.999, 0.999,
        0.001, 0.001, 0.001, 0.001
    ]
    for test_result, test_payload in zip(test_results, test_payloads):
        event = {'body' : json.dumps(test_payload)}
        resp = handler(event, None)
        score = eval(resp['body'])['all_comparisons'][0]
        print(score, test_result)
        test_comparison_strings(result=score, expected_result=test_result)
