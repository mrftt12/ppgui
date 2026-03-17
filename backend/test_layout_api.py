import pytest
from fastapi.testclient import TestClient
from server import app, networks_store
import multiconductor as mc

client = TestClient(app)


def test_update_layout():
    # Setup network
    net = mc.create_empty_network()
    mc.create_bus(net, vn_kv=20.0, name="Bus 0")
    mc.create_bus(net, vn_kv=0.4, name="Bus 1")
    network_id = "test-layout-net"
    networks_store[network_id] = net

    # Get initial bus indices
    buses = sorted(set(net.bus.index.get_level_values(0)))
    assert len(buses) >= 2

    bus1 = int(buses[0])
    bus2 = int(buses[1])

    # New positions
    layout_update = {
        str(bus1): {"x": 100.5, "y": 200.5},
        str(bus2): {"x": 300.0, "y": 400.0},
    }

    response = client.put(f"/api/networks/{network_id}/layout", json=layout_update)
    assert response.status_code == 200

    # Verify updates in network
    assert net.bus_geodata.loc[bus1, "x"] == 100.5
    assert net.bus_geodata.loc[bus1, "y"] == 200.5
    assert net.bus_geodata.loc[bus2, "x"] == 300.0
    assert net.bus_geodata.loc[bus2, "y"] == 400.0


if __name__ == "__main__":
    test_update_layout()
    print("test_update_layout passed")
