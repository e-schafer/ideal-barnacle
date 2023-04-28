"""
test
"""
import json
from pprint import pprint as pp


def parse_me(data: str):
    """
    This function parses the data from the kafka message
    """
    js_data = json.loads(data)
    main_key = str(list(js_data.keys())[0])
    return {**js_data[main_key], "dtype": main_key}


if __name__ == "__main__":
    in_data = """{"cat":{"weight":3,"height":2,"age":1,"color":"black"}}"""
    data = parse_me(in_data)
    pp(data)
