import pickle
import os
import sys

# Ensure parent directory is in path to import multiconductor
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import multiconductor as mc

NETWORKS_DIR = "networks"


def verify_network(net_name):
    pkl_path = os.path.join(NETWORKS_DIR, f"{net_name}.pkl")
    if not os.path.exists(pkl_path):
        print(f"Network file not found: {pkl_path}")
        return

    with open(pkl_path, "rb") as f:
        net = pickle.load(f)

    print(f"Network: {net_name}")
    print(f"Buses: {len(net.bus)}")
    print(f"Lines: {len(net.line)}")

    if "ext_grid" in net:
        print(f"Sources (Ext Grid): {len(net.ext_grid)}")
    else:
        print("Sources: None")

    if "asymmetric_shunt" in net and net.asymmetric_shunt is not None:
        print(f"Shunts (Asymmetric): {len(net.asymmetric_shunt)}")

    if "asymmetric_gen" in net and net.asymmetric_gen is not None:
        print(f"Generators (Asymmetric): {len(net.asymmetric_gen)}")

    if "ext_grid" in net and len(net.ext_grid) > 0:
        slacks = net.ext_grid.bus.unique()
        print(f"Slack Buses: {slacks}")
        # Check lines connected to slack
        connected_lines = net.line[
            (net.line.from_bus.isin(slacks)) | (net.line.to_bus.isin(slacks))
        ]
        print(f"Lines connected to Slack: {len(connected_lines)}")
        if len(connected_lines) > 0:
            print("First connected line:")
            print(connected_lines.iloc[0])
            print("Phases of first line:")
            print(f"From: {connected_lines.iloc[0]['from_phase']}")
            print(f"To: {connected_lines.iloc[0]['to_phase']}")


if __name__ == "__main__":
    print("--- ALMOND_12KV ---")
    verify_network("ALMOND_12KV")
    print("\n--- ANAHURST_4KV ---")
    verify_network("ANAHURST_4KV")
