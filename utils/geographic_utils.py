import json
from functools import lru_cache
import sqlalchemy
import os
import pandas as pd
import ast
import boto3

def get_boto_session():
    return boto3.Session(region_name='us-west-2')

def __pull_global_secret(varname, boto_client):
    secret_vars_dict = ast.literal_eval(boto_client.get_secret_value(SecretId=varname)['SecretString'])
    if 'value' in secret_vars_dict:
        return secret_vars_dict['value']
    raise Exception()

def get_env_var(varname):
    if varname in os.environ:
        val = os.environ[varname]
        return val

    secret_name = 'DS_MS_SECRETS'  # "dev-internal-tool-secrets"
    client = get_boto_session().client(service_name='secretsmanager')
    secret_vars_dict = ast.literal_eval(client.get_secret_value(SecretId=secret_name)['SecretString'])

    if varname in secret_vars_dict:
        # logger.info(f'secret_vars_dict={secret_vars_dict}')
        val = secret_vars_dict[varname]
        if not val.startswith('GLOBAL_'):
            return val
        # We need to pull the global secret then
        res_val = __pull_global_secret(val, client)
        return res_val
    else:
        return None

def _postgres_engine(
    user: str, password: str, host: str, name: str, echo: bool = False
) -> sqlalchemy.engine.Engine:
    """Create a postgres sql engine.

    :param user: User name
    :param password: Password
    :param host: Host
    :param name: Name
    :param echo: sqlalchemy echo

    :return: A postgres engine
    """
    login = f"postgresql://{user}:{password}@{host}:5432/{name}"
    return sqlalchemy.create_engine(login, echo=echo)



def _rapid_db_conn(
    host_env_variable: str, echo: bool = False
) -> sqlalchemy.engine.Engine:
    """
    Norm cluster endpoint engine.

    :param host_env_variable: Then name of the environment variable for the host
    :param echo: sqlalchemy echo

    .. csv-table:: Environment Variables

       DB_RAPID_USER
       DB_RAPID_PASSWORD
       DB_RAPID_HOST_RO
       DB_RAPID_NAME

    :return: A sql engine to the rapid DB"
    """
    db_rapid_user = get_env_var("DB_RAPID_USER")
    db_rapid_password = get_env_var("DB_RAPID_PASSWORD")
    db_rapid_host = get_env_var(host_env_variable)
    db_rapid_name = get_env_var("DB_RAPID_NAME")
    if not (db_rapid_user and db_rapid_password and db_rapid_host and db_rapid_name):
        Exception("Unable to construct database connection")
    return _postgres_engine(
        db_rapid_user,  # pyright: ignore
        db_rapid_password,  # pyright: ignore
        db_rapid_host,  # pyright: ignore
        db_rapid_name,  # pyright: ignore
        echo,  # pyright: ignore
    )

def rapid_db_reader_conn(echo: bool = False) -> sqlalchemy.engine.Engine:
    """Norm cluster read-only endpoint engine.

    :param echo: sqlalchemy echo
    .. todo::

       For security reasons this should be calling KMS rather than hard-coded creds.

    .. csv-table:: **Environment Variables**

       DB_RAPID_USER
       DB_RAPID_PASSWORD
       DB_RAPID_HOST_RO
       DB_RAPID_NAME

    :return: A read-writ sql engine to the rapid DB
    """
    return _rapid_db_conn("DB_RAPID_HOST_RO", echo)


def pull_state_map():
    file = json.load(open('utils/state_abbrev_mapping.json', 'rb'))
    return file

def pull_geotags(cpli):
    conn = rapid_db_reader_conn()
    query = f"""
    SELECT
        id AS cpli,
        address_components ->> 'city' AS city,
        address_components ->> 'state' AS state,
        address_components ->> 'route' AS street
    FROM address_keys
    WHERE id::BIGINT = {cpli}
    LIMIT 1
    """
    cpli = int(cpli) if isinstance(cpli, str) else cpli # ensure typing of CPLI
    geotags = pd.read_sql(sql=query, con=conn).set_index('cpli').to_dict(orient='index')
    # add in the long state version to the geotag dictonary
    state_map = pull_state_map()
    full_state = state_map[geotags[cpli]['state'].title()]
    geotags[cpli]['full_state'] = full_state
    geotokens = list(geotags[cpli].values())
    # remove any null values from the geotags
    geotokens = [token.lower() for token in geotokens if token is not None]
    return geotokens


@lru_cache(maxsize=2048)
def read_hospital_cplis():
    file = 'utils/hospital_cplis.txt'
    with open(file) as f:
        hospital_cplis = [int(i.strip()) for i in f.read().split(',')]

    return hospital_cplis

def is_hospital_cpli(cpli):
    hospitals = read_hospital_cplis()
    return True if cpli in hospitals else False

def is_geotag_diff(s1, s2, cpli):
    geotags = [i.lower() for i in pull_geotags(cpli)]
    string_diff = list(set(s1.lower().split()) - set(s2.lower().split())) + list(set(s2.lower().split()) - set(s1.lower().split()))
    string_diff = set(string_diff)
    string_diff_words = ' '.join(sorted(string_diff))
    for geotag in geotags:
        geotag_words = ' '.join(sorted(geotag.split()))
        if geotag_words == string_diff_words:
            return True
    return False

