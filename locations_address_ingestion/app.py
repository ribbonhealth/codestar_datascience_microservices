import json
import pandas as pd
import requests
import urllib
import uuid
from utils.db_utils import (
    norm_db_conn,
    rapid_db_conn,
    get_env_var,
    norm_db_reader_conn
)

from locations_address_ingestion.location_resolver import parse_address_components, format_hw_place_json


def return_geocode_error(msg, address):
    return {
        'statusCode': 400,
        'body': {
            'address_input': address,
            'errorType': 'Issue with Google Maps GeoCode',
            'error': str(msg)
        }}


def return_format_error():
    return {
        'statusCode': 400,
        'body': {
            "ERROR": "Format"
        }}


def check_geocode_exists(full_address, conn):
    print('querying db for address')
    # first search locations_geocode table
    # exists_query = """select lkr_id,lkr_parent_id,coalesce_parent_lkr_id 
    # from locations_geocode where value_src='{0}'""".format(full_address.replace("'","''"))

    exists_query = f"""
    with a as (
        select location_keys_rollup_id
        from location_mappings
        where value_src='{full_address.replace("'", "''")}'
    ) select location_keys_rollup_id as lkr_id,
             parent_id as lkr_parent_id,
             coalesce(parent_id,id) as coalesce_parent_lkr_id,
             address as geocoded_address,
             uuid::text,
             latitude::float,
             longitude::float,
             address_components
    from a join location_keys_rollup on id = location_keys_rollup_id
    limit 1"""
    coded = pd.read_sql(exists_query, conn).to_dict(orient='records')
    print('query finished')

    if coded:
        return coded[0]

    return False


def check_lkr(formatted_address, conn):
    # lkr_query = """select id as lkr_id, parent_id as lkr_parent_id, coalesce(parent_id, id) as coalesce_parent_lkr_id 
    #             from location_keys_rollup_test where address='{0}'""".format(formatted_address.replace("'","''"))

    lkr_query = f"""
    select id as lkr_id,
           parent_id as lkr_parent_id,
           coalesce(parent_id, id) as coalesce_parent_lkr_id,
           address as geocoded_address,
           uuid::text,
           latitude::float,
           longitude::float,
           address_components
    from location_keys_rollup
    where address='{formatted_address.replace("'", "''")}'
    limit 1"""

    lkr_data = pd.read_sql(lkr_query, conn).to_dict(orient='records')

    if lkr_data:
        return lkr_data[0]

    return False


def geocode_address(full_address):
    # then run the geocode if doesn't exist
    GOOGLE_MAPS_KEY = get_env_var(varname='GOOGLE_MAPS_KEY')
    GOOGLE_SEARCH_URL = f"https://maps.googleapis.com/maps/api/geocode/json?%s&key={GOOGLE_MAPS_KEY}"
    google_search_request = GOOGLE_SEARCH_URL % (urllib.parse.urlencode({"address": full_address}))

    raw_response = json.loads(requests.get(google_search_request, timeout=30).text)
    print(raw_response)
    return raw_response


def parse_geocode_obj(raw_response, full_address):
    """
    raw_response: is of type JSON with the following schema
    {
        "results": [
            {
                "address_components": [
                    {
                        "long_name": string
                        "short_name": string
                        "types": [
                            string
                        ]
                    }
                ],
                "formatted_address": string,
                "geometry": {
                    "location": {
                        "lat": float,
                        "lng": float
                    },
                    "location_type": string,
                    "viewport": {
                        "northeast": {
                            "lat": float,
                            "lng": float
                        },
                        "southwest": {
                            "lat": float,
                            "lng": float
                        }
                    },
                    "place_id": string,
                    types: [
                        string
                    ]
                }
            }
        ],
        "status": string
    }

    """
    if raw_response and "results" in raw_response:

        geocoded_result = {
            'value_src': full_address,
            'raw_response': json.dumps(raw_response["results"]),
            'data': None,
            'place_id': None,
            'updated_at': str(pd.Timestamp('now')),
            'address': None,
            'status': 1,
            'location_keys_rollup_id': None,
            'address_components': None,
        }

        if len(raw_response["results"]) >= 1:
            geocoded_result['status'] = 0
            chosen_google_location = raw_response["results"][0]
            geocoded_result['data'] = json.dumps(chosen_google_location)
            geocoded_result['place_id'] = chosen_google_location['place_id']
            geocoded_result['address'] = chosen_google_location['formatted_address']

            address_components = parse_address_components(chosen_google_location)
            geocoded_result['address_components'] = json.dumps(address_components)
            return geocoded_result, address_components
        else:
            return False, "Geocode response had zero results"
    else:
        return False, "Geocode had no results" + (
            ': ' + raw_response["error_message"] if raw_response and "error_message" in raw_response else '')


def add_new_location(worker_cursor, google_obj, address_components, parent_id, rapid_cursor):
    insert_rows = []
    address_uuid = str(uuid.uuid4())
    name = google_obj["name"] if "name" in google_obj else None

    if 'geometry' in google_obj:
        latitude = google_obj["geometry"]["location"]["lat"]
        longitude = google_obj["geometry"]["location"]["lng"]
    else:
        latitude = json.loads(google_obj["data"])["geometry"]["location"]["lat"]
        longitude = json.loads(google_obj["data"])["geometry"]["location"]["lng"]
    hw_place_json = format_hw_place_json(address_components=address_components, uuid=address_uuid, lat=latitude,
                                         lng=longitude, name=name)

    if parent_id:
        insert_rows.append((address_uuid, latitude, longitude, address_components["formatted_address"],
                            json.dumps(google_obj), json.dumps(hw_place_json), json.dumps(address_components),
                            parent_id))
        values = b','.join(
            worker_cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])) for x
            in insert_rows)
        worker_cursor.execute("""
            INSERT INTO location_keys_rollup (uuid, latitude, longitude, address, google_object, hw_place_json, address_components, parent_id)
            VALUES """ + str(values, 'utf-8') + " ON CONFLICT DO NOTHING returning id")
        id_resp = worker_cursor.fetchall()
        values = b','.join(worker_cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                                 (id_resp[0][0], x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])) for x
                           in insert_rows)
        rapid_cursor.execute("""
            INSERT INTO address_keys (id, uuid, latitude, longitude, address, google_object, hw_place_json, address_components, parent_id)
            VALUES """ + str(values, 'utf-8') + " ON CONFLICT DO NOTHING")
    else:
        insert_rows.append((address_uuid, latitude, longitude, address_components["formatted_address"],
                            json.dumps(google_obj), json.dumps(hw_place_json), json.dumps(address_components)))
        values = b','.join(
            worker_cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", (x[0], x[1], x[2], x[3], x[4], x[5], x[6])) for x in
            insert_rows)
        worker_cursor.execute("""
            INSERT INTO location_keys_rollup (uuid, latitude, longitude, address, google_object, hw_place_json, address_components)
            VALUES """ + str(values, 'utf-8') + " ON CONFLICT DO NOTHING returning id")
        id_resp = worker_cursor.fetchall()
        values = b','.join(worker_cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)",
                                                 (id_resp[0][0], x[0], x[1], x[2], x[3], x[4], x[5], x[6])) for x in
                           insert_rows)
        rapid_cursor.execute("""
            INSERT INTO address_keys (id, uuid, latitude, longitude, address, google_object, hw_place_json, address_components)
            VALUES """ + str(values, 'utf-8') + " ON CONFLICT DO NOTHING")


def add_new_location_child(worker_cursor, conn, google_obj, address_components, rapid_cursor):
    # check if the parent exists, else create the parent first
    parent_address = (address_components['street_number'] or "") + ' '
    parent_address += ', '.join([(address_components['route'] or ""),
                                 (address_components['city'] or ""),
                                 (address_components['state'] or ""),
                                 (address_components['zip'] or ""),
                                 (address_components['country'] or "")])

    check = check_geocode_exists(parent_address, conn)
    if check:
        parent_id = check['lkr_id']
    else:
        parent_raw_response = geocode_address(parent_address)
        parent_parsed_google_obj, parent_address_components = parse_geocode_obj(parent_raw_response, parent_address)

        # if the location is the same as the parent, then only add one
        if parent_parsed_google_obj and parent_parsed_google_obj['place_id'] == google_obj['place_id']:
            parent_id = False
        else:
            # check if the object exists in lkr
            lkr_exists = check_lkr(parent_address_components['formatted_address'], conn)
            if lkr_exists:
                parent_id = lkr_exists['lkr_id']
            else:
                # insert location then get the parent id
                add_new_location(worker_cursor, parent_parsed_google_obj, parent_address_components, False,
                                 rapid_cursor)
                parent_id = check_lkr(parent_address_components['formatted_address'], conn)['lkr_id']

    add_new_location(worker_cursor, google_obj, address_components, parent_id, rapid_cursor)


def _build_response_object(lkr_exists_object):
    return_elements = {
        'lkr_id': None,
        'lkr_parent_id': None,
        'coalesce_parent_lkr_id': None,
        'geocoded_address': None,
        'latitude': None,
        'longitude': None,
        'address_components': None,
    }
    [return_elements.update({k: v}) for k, v in lkr_exists_object.items()]
    return return_elements


def handler(event, context=None):
    # right now handling full address, can move to concatenation down the road
    try:
        req_data = json.loads(event['body'])
        full_address = req_data['full_address']
    except:
        return return_format_error()

    conn, pconn = norm_db_conn()
    rapid_conn, rapid_pconn = rapid_db_conn()
    reader_conn = norm_db_reader_conn()

    check = check_geocode_exists(full_address, reader_conn)
    if check:
        return {
            'statusCode': 200,
            'body': json.dumps(check)
        }
    else:
        try:
            raw_response = geocode_address(full_address)
        except Exception as e:
            return return_geocode_error(e, full_address)

        parsed_google_obj, address_components = parse_geocode_obj(raw_response, full_address)
        print(address_components['formatted_address'])
        if parsed_google_obj:
            # Use the formatted address from Google to see if the location key rollup already exists. Directly
            # queries location_keys_rollup table rather than going through location_mappings
            lkr_exists = check_lkr(address_components['formatted_address'], reader_conn)
            if lkr_exists:
                # If it does, replace all return elements with values retrieved from the existing location key
                parsed_google_obj['location_keys_rollup_id'] = lkr_exists['lkr_id']
                resp = _build_response_object(lkr_exists)
            else:
                # If it does not, add it as a location in location_keys_rollup
                add_new_location_child(pconn.cursor(), conn, json.loads(parsed_google_obj['data']), address_components, rapid_pconn.cursor())
                lkr_exists = check_lkr(address_components['formatted_address'], conn)
                #[parsed_google_obj.update({k:v}) for k,v in lkr_exists.items()]
                parsed_google_obj['location_keys_rollup_id'] = lkr_exists['lkr_id']
                resp = _build_response_object(lkr_exists)

            #pd.DataFrame([parsed_google_obj]).to_sql('locations_geocode', conn, method='multi', index=False, if_exists='append')
            pd.DataFrame([parsed_google_obj]).to_sql('location_mappings', conn, method='multi', index=False, if_exists='append')

            pconn.close()
            conn.dispose()
            rapid_pconn.close()
            rapid_conn.dispose()
            reader_conn.dispose()

            return {
                'statusCode': 200,
                'body': json.dumps(resp)
            }
        else:
            pconn.close()
            conn.dispose()
            rapid_pconn.close()
            rapid_conn.dispose()
            reader_conn.dispose()

            return return_geocode_error(address_components, full_address)


# if __name__ == '__main__':
#     body = {"body": "{\"full_address\": \"335 Brighton Ave Ste 201, Portland, ME 04102\"}"}
#     return_value = handler(body)
#     print(return_value)
