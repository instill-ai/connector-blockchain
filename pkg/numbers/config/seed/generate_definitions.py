import jsonref
import json
import copy

from os.path import dirname

NAME = "Numbers Protocol"
TASKS = ['default']

DEFINITION_TEMPLATE = [
    {
        "custom": False,
        "documentation_url": "https://www.instill.tech/docs/vdp/blockchain-connectors/numbers",
        "icon": "numbers.svg",
        "icon_url": "",
        "id": "blockchain-numbers",
        "public": True,
        "spec": {},
        "title": NAME,
        "tombstone": False,
        "uid": "70d8664a-d512-4517-a5e8-5d4da81756a7",
        "vendor_attributes": {}
    }
]


OPENAPI_SCHEMA_TEMPLATE = {
    "openapi": "3.0.0",
    "info": {
        "version": "1.0.0",
        "title": f"{NAME} Connector OpenAPI"
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
    openapi_schema_encode = json.dumps(OPENAPI_SCHEMA_TEMPLATE)
    openapi_schema_encode = openapi_schema_encode.replace(
        "task_name", task_name)
    return json.loads(openapi_schema_encode)


def convert_field(field):
    field = copy.deepcopy(field)
    if "const" in field:
        return field

    if "type" not in field or field["type"] == "object":
        if "properties" in field:
            for k, v in field["properties"].items():
                field["properties"][k] = convert_field(v)
        if "allOf" in field:
            for i in range(len(field["allOf"])):
                field["allOf"][i] = convert_field(field["allOf"][i])
        if "anyOf" in field:
            for i in range(len(field["anyOf"])):
                field["anyOf"][i] = convert_field(field["anyOf"][i])
        if "oneOf" in field:
            for i in range(len(field["oneOf"])):
                field["oneOf"][i] = convert_field(field["oneOf"][i])
    else:
        if "instillFormat" not in field:
            return field
        assert ("instillFormat" in field)
        assert ("instillUpstreamTypes" in field)
        instill_upstream_types = field["instillUpstreamTypes"]
        original_field = copy.deepcopy(field)
        original_field.pop("title", None)
        original_field.pop("description", None)
        original_field.pop("instillFormat", None)
        original_field.pop("instillUpstreamTypes", None)

        field = {
            "title": field["title"],
            "description": field.get("description", ""),
            "instillFormat": field["instillFormat"],
            "instillUpstreamTypes": field["instillUpstreamTypes"],
            "oneOf": []
        }

        for instillUpstreamType in instill_upstream_types:
            if instillUpstreamType == "value":
                original_field["instillUpstreamType"] = instillUpstreamType
                field["oneOf"].append(original_field)
            if instillUpstreamType == "reference":
                field["oneOf"].append({
                    "type": "string",
                    "pattern": "^\\{.*\\}$",
                    "instillUpstreamType": instillUpstreamType
                })

    return field


def convert_data_schema_to_component_schema(data):
    component_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": f"{NAME} Component Setting",
        "type": "object"
    }
    if len(TASKS) > 1:
        component_schema["properties"] = {
            "input": {
                "properties": {
                    "task": {
                        "title": "Task",
                        "enum": TASKS
                    }
                }
            }
        }
        component_schema["allOf"] = []
        for task in TASKS:
            data[task]["input"]["title"] = "Data Flow"

            component_schema["allOf"].append({
                "if": {
                    "properties": {
                        "input": {
                            "properties": {
                                "task": {
                                    "const": task
                                }
                            }
                        }
                    }
                },
                "then": {
                    "properties": {
                        "metadata": {
                            "title": "Metadata",
                            "type": "object"
                        },
                        "input": convert_field(data[task]["input"])
                    }
                }
            })
    else:
        data["default"]["input"]["title"] = "Data Flow"
        component_schema["properties"] = {
            "metadata": {
                "title": "Metadata",
                "type": "object"
            },
            "input": convert_field(data["default"]["input"])
        }


    return component_schema


if __name__ == "__main__":

    base_path = dirname(__file__)
    base_uri = "file://{}/".format(base_path)

    openapi_schema = {}
    for task in TASKS:
        openapi_schema[task] = gen_openapi(task)

    with open("./resource.json", "r") as rsc_file:
        resource = json.load(rsc_file)
    with open("./data.json", "r") as data_file:
        data = jsonref.load(data_file, base_uri=base_uri,
                            jsonschema=True, merge_props=True)

    DEFINITION_TEMPLATE[0]["spec"]["resource_specification"] = resource
    DEFINITION_TEMPLATE[0]["spec"]["component_specification"] = convert_data_schema_to_component_schema(
        data)
    DEFINITION_TEMPLATE[0]["spec"]["openapi_specifications"] = openapi_schema

    a = jsonref.loads(json.dumps(DEFINITION_TEMPLATE), base_uri=base_uri,
                      jsonschema=True, merge_props=True)

    with open("../definitions.json", "w") as o:
        out = json.dumps(a, indent=2)
        o.write(out)
        o.write("\n")
