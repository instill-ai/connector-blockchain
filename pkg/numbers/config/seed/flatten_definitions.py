import jsonref
import json

from os.path import dirname

openapi_schema_template = {
    "openapi": "3.0.0",
    "info": {
        "version": "1.0.0",
        "title": "Numbers Connector Openapi"
    },
    "paths": {
        "/execute": {
            "post": {
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "inputs": {
                                        "type": "array",
                                        "items": {
                                            "$ref": "data.json#/task_name/input"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "outputs": {
                                            "type": "array",
                                            "items": {
                                                "$ref": "data.json#/task_name/output"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


def gen_openapi(task_name):
    openapi_schema_encode = json.dumps(openapi_schema_template)
    openapi_schema_encode = openapi_schema_encode.replace(
        "task_name", task_name)
    return json.loads(openapi_schema_encode)


base_path = dirname(__file__)
base_uri = 'file://{}/'.format(base_path)

openapi_schema = {}
openapi_schema['default'] = gen_openapi('default')
with open("./openapi.json", 'w') as openapifile:
    json.dump(openapi_schema, openapifile, indent=2)


with open("./definitions.json") as schema_file:
    a = jsonref.loads(schema_file.read(), base_uri=base_uri,
                      jsonschema=True, merge_props=True)

with open('../definitions.json', 'w') as o:
    json.dump(a, o, indent=2)
