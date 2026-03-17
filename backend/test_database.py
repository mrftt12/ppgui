import os
import uuid
from database import Database

DB_PATH = "test_loadflow.db"


def test_create_network(db):
    net_id = str(uuid.uuid4())
    db.create_network(net_id, "Test Net")
    networks = db.list_networks()
    assert len(networks) == 1
    assert networks[0]["id"] == net_id
    assert networks[0]["name"] == "Test Net"


def test_save_and_load_version(db):
    net_id = str(uuid.uuid4())
    db.create_network(net_id, "Test Net")

    data_v1 = {"bus": [{"name": "Bus 1", "vn_kv": 20}]}
    v1 = db.save_version(net_id, data_v1, "v1")
    assert v1 == 1

    data_v2 = {"bus": [{"name": "Bus 1", "vn_kv": 20}, {"name": "Bus 2"}]}
    v2 = db.save_version(net_id, data_v2, "v2")
    assert v2 == 2

    # Load v1
    loaded_v1 = db.load_version(net_id, version_num=1)
    assert len(loaded_v1["bus"]) == 1

    # Load v2
    loaded_v2 = db.load_version(net_id, version_num=2)
    assert len(loaded_v2["bus"]) == 2

    # Get History
    history = db.get_history(net_id)
    assert len(history) == 2
    assert history[0]["version"] == 2


if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    db = Database(DB_PATH)
    try:
        test_create_network(db)
        test_save_and_load_version(db)
        print("Tests Passed")
    except Exception as e:
        print(f"Tests Failed: {e}")
        exit(1)
    finally:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
