import json
from collections import defaultdict


def inspect_keys(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)

    keys_by_type = defaultdict(set)

    # Check if data is a list or dict with a specific key holding the list
    objects = []
    if isinstance(data, list):
        objects = data
    elif isinstance(data, dict):
        if "objects" in data:
            objects = data["objects"]
        else:
            # Maybe it is a dict of objects?
            print(f"Root keys: {list(data.keys())}")
            # If it has 'powerflow.node' etc as keys, iterate them
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            keys_by_type[key].update(item.keys())
                elif isinstance(value, dict):
                    keys_by_type[key].update(value.keys())

    if objects:
        for obj in objects:
            obj_type = obj.get("object") or obj.get("class") or "unknown"
            keys_by_type[obj_type].update(obj.keys())

    for obj_type, keys in keys_by_type.items():
        print(f"Type: {obj_type}")
        print(f"  Keys: {sorted(list(keys))}")
        print("-" * 20)


inspect_keys("/Users/admin/git/ppmc4sce-main/tcgm-gis-converted.json")
