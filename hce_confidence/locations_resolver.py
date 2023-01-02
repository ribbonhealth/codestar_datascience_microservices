import re
from urllib.parse import quote

def has_key_floor_word(input):
    key_words = {
        "GROUND": "G",
        "G": "G",
        "BASEMENT": "B",
        "B": "B"
    }
    if input in key_words.keys():
        return key_words[input]
    return None


def has_numbers(input):
    return bool(re.search(r'\d', input))


def validate_floor(token):
    floor_ordinal = ['1ST','2ND','3RD','4TH','5TH','6TH','7TH','8TH','9TH','10TH','11TH','12TH','13TH','14TH','15TH',
                     '16TH','17TH','18TH','19TH','20TH','21ST','22ND','23RD','24TH','25TH','26TH','27TH','28TH','29TH',
                     '30TH','31ST','32ND','33RD','34TH','35TH','36TH','37TH','38TH','39TH','40TH','41ST','42ND','43RD',
                     '44TH','45TH','46TH','47TH','48TH','49TH','50TH','51ST','52ND','53RD','54TH','55TH','56TH','57TH',
                     '58TH','59TH','60TH','61ST','62ND','63RD','64TH','65TH','66TH','67TH','68TH','69TH','70TH','71ST',
                     '72ND','73RD','74TH','75TH','76TH','77TH','78TH','79TH','80TH','81ST','82ND','83RD','84TH','85TH',
                     '86TH','87TH','88TH','89TH','90TH','91ST','92ND','93RD','94TH','95TH','96TH','97TH','98TH','99TH']
    floor_map = {}
    for i, v in enumerate(floor_ordinal):
        floor_map[v] = str(i+1)
    
    floor = None
    if token.isdigit():
        floor = token
    elif token in floor_map:
        floor = floor_map[token]
    elif has_numbers(token):
        floor = token
    elif has_key_floor_word(token):
        floor = token
    return floor


def get_floor(next_token, prev_token):
    floor = None
    if next_token:
        floor = validate_floor(next_token)
    if not floor and prev_token:
        floor = validate_floor(prev_token)
    if not floor:
        if next_token:
            floor = next_token
        elif prev_token:
            floor = prev_token
    return floor

def format_street(street):
    if not street:
        return None
    else:
        abbreviation_map = {
            "avenue": "Ave",
            "street": "St",
            "boulevard": "Blvd",
            "court": "Ct",
            "drive": "Dr",
            "parkway": "Pkwy",
            "road": "Rd",
            "lane": "Ln"
        }

        street_components = list(filter(None, street.split(" ")))
        last_street_component = street_components[-1].lower()
        for abbreviation in abbreviation_map.keys():
            if last_street_component == abbreviation:
                street_components[-1] = abbreviation_map[abbreviation]
                break
        return " ".join(street_components)


def clean(token):
    token = token.strip()
    if token != "#":
        token = token.replace("#", "")
    token = token.replace(",", "")
    token = token.replace(".", "")
    token = token.upper()
    return token


def format_subpremise(components):
    if not components:
        return None
    else:
        subprem_designator_sort = ["BLDG", "DEPT", "FL", "OFC", "RM", "UNIT", "APT", "STE", "#"]
        formatted_string = ""
        component_keys = set(components.keys())
        for key in subprem_designator_sort:
            if key in component_keys:
                formatted_key = key
                if key in ["UNIT", "APT", "STE", "RM", "#"]:
                    formatted_key = "#"
                if components[key]:
                    designator = formatted_key + " " + components[key]
                    if not formatted_string:
                        formatted_string = designator
                    else:
                        formatted_string += ", " + designator
        return formatted_string


def resolve_subpremise(subpremise):
    subpremise_tokens = subpremise.split()
    subpremise_components = dict()
    token_length = len(subpremise_tokens)
    for idx, token in enumerate(subpremise_tokens):
        cleansed_token = clean(token)
        next_token = clean(subpremise_tokens[idx + 1]) if token_length > idx + 1 else None
        prev_token = clean(subpremise_tokens[idx - 1]) if idx > 0 else None
        if cleansed_token in ["STE", "SUITE", "SUIT"]:
            subpremise_components["STE"] = next_token
        elif cleansed_token in ["APT", "APARTMENT"]:
            subpremise_components["APT"] = next_token
        elif cleansed_token in ["UNIT"]:
            subpremise_components["UNIT"] = next_token
        elif cleansed_token in ["RM", "ROOM"]:
            subpremise_components["RM"] = next_token
        elif cleansed_token in ["OF", "OFC", "OFFICE"]:
            subpremise_components["OFC"] = next_token
        elif cleansed_token in ["FL", "FLOOR", "LEVEL"]:
            floor = get_floor(next_token, prev_token)
            subpremise_components["FL"] = floor
        elif cleansed_token in ["BUILDING", "BLDG"]:
            subpremise_components["BLDG"] = next_token
        elif cleansed_token in ["DEPT", "DEPARTMENT"]:
            subpremise_components["DEPT"] = next_token
        elif cleansed_token in ["#"]:
            subpremise_components["#"] = next_token
        else:
            if not subpremise_components and token_length == 1:
                subpremise_components["#"] = cleansed_token

    subpremise_components["formatted_subpremise"] = format_subpremise(subpremise_components)
    return subpremise_components


def format_location(components):
    formatted_address = ""
    if components["street_number"]:
        formatted_address = components["street_number"]
    if components["route"]:
        if formatted_address:
            formatted_address += " " + components["route"]
        else:
            formatted_address = components["route"]
    if components["intersection"]:
        formatted_address += components["intersection"]

    # need to format subpremise correctly
    if components["subpremise"] and components["subpremise"]["formatted_subpremise"]:
        formatted_address += " " + components["subpremise"]["formatted_subpremise"] + ", "
    else:
        if components["street_number"] or components["route"] or components["intersection"]:
            formatted_address += ", "

    if components["city"]:
        formatted_address += components["city"] + ", "
    if components["state"]:
        formatted_address += components["state"] + " "
    if components["zip"]:
        formatted_address += components["zip"] + ", "
    if components["country"]:
        formatted_address += components["country"]
    return formatted_address


def parse_address_components(google_place_object):
    """
    :param google_place_object: a Google Place object
    :return: parse JSON object of address components we want
    """
    formatted_components = {
        "subpremise": None,
        "street_number": None,
        "intersection": None,
        "route": None,
        "city": None,
        "state": None,
        "zip": None,
        "country": None
    }
    if "address_components" in google_place_object:
        address_components = google_place_object["address_components"]
    else:
        address_components = []

    is_puerto_rico_location = "Puerto Rico" in google_place_object["formatted_address"]
    for component in address_components:
        if "types" in component:
            if "subpremise" in component["types"]:
                formatted_components["subpremise"] = resolve_subpremise(component["short_name"])
            elif "street_number" in component["types"]:
                formatted_components["street_number"] = component["short_name"]
            elif "route" in component["types"]:
                formatted_components["route"] = format_street(component["short_name"])
            elif "intersection" in component["types"]:
                formatted_components["intersection"] = format_street(component["short_name"])
            elif "postal_code" in component["types"]:
                formatted_components["zip"] = component["short_name"]
            elif "locality" in component["types"]:
                city = component["long_name"]
                if "Saint " in city or "Mount " in city:
                    city = component["short_name"]
                formatted_components["city"] = city
            elif "sublocality" in component["types"] and not formatted_components["city"]:
                city = component["long_name"]
                if "Saint " in city or "Mount " in city:
                    city = component["short_name"]
                formatted_components["city"] = city
            elif "administrative_area_level_1" in component["types"]:
                if is_puerto_rico_location:
                    formatted_components["state"] = "PR"
                else:
                    formatted_components["state"] = component["short_name"]
            elif "country" in component["types"]:
                if is_puerto_rico_location:
                    formatted_components["country"] = "US"
                else:
                    formatted_components["country"] = component["short_name"]

    if formatted_components["street_number"] and (
            formatted_components["route"] or formatted_components["intersection"]) and \
            formatted_components["city"] and formatted_components["state"] and formatted_components["zip"]:
        formatted_components["is_tokenized"] = True
    else:
        formatted_components["is_tokenized"] = False

    formatted_components["formatted_address"] = format_location(formatted_components)
    return formatted_components
    
def format_hw_place_json(address_components, uuid, lat, lng, name):
    new_address = address_components["formatted_address"]
    subpremise = address_components["subpremise"]["formatted_subpremise"] if address_components["subpremise"] else None
    city = address_components["city"]
    state = address_components["state"]
    zip_ = address_components["zip"]
    intersection = address_components["intersection"]
    street_number = address_components["street_number"]
    route = address_components["route"]
    if subpremise and route and street_number:
        new_street = street_number + " " + route + " " + subpremise
    elif street_number and route:
        new_street = street_number + " " + route
    elif subpremise and route:
        new_street = route + " " + subpremise
    elif route:
        new_street = route
    elif subpremise:
        new_street = subpremise
    else:
        new_street = None

    if street_number and route:
        address_line_1 = street_number + " " + route
    elif route:
        address_line_1 = route
    elif intersection:
        address_line_1 = intersection
    else:
        address_line_1 = None

    google_maps_link = "https://www.google.com/maps/@" + str(lat) + "," + str(lng) + "?q=" + quote(new_address)

    formatted_hw_place = {
        "uuid": uuid,
        "name": name,
        "address": new_address,
        "address_details": {
            "street": new_street,
            "address_line_1": address_line_1,
            "address_line_2": subpremise,
            "city": city,
            "state": state,
            "zip": zip_
        },
        "latitude": lat,
        "longitude": lng,
        "google_maps_link": google_maps_link,
    }
    return formatted_hw_place
