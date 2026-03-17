import pickle
import os
import sys

# Add parent dir for multiconductor
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import multiconductor as mc
import pandapower as pp

pkl_path = "networks/ALMOND_12KV.pkl"

if os.path.exists(pkl_path):
    with open(pkl_path, "rb") as f:
        net = pickle.load(f)
    print(f"Network: ALMOND_12KV")
    print(f"Buses: {len(net.bus)}")
    print(f"Lines: {len(net.line)}")
    print(f"Sources (Ext Grid): {len(net.ext_grid)}")

    if len(net.ext_grid) > 0:
        print("First Ext Grid:")
        print(net.ext_grid.iloc[0])
else:
    print(f"File {pkl_path} not found.")
