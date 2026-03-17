import pandapower as pp
import os
import json

file_path = "output_pandapower/SUNFLOWER_12KV.json"

if not os.path.exists(file_path):
    print(f"Error: {file_path} not found.")
    exit(1)

try:
    print(f"Loading {file_path}...")
    net = pp.from_json(file_path)
    print("Network loaded successfully.")
    print(f"Buses: {len(net.bus)}")
    print(f"Lines: {len(net.line)}")
    print(f"Loads: {len(net.load) if 'load' in net else 0}")
    # Multiconductor uses asymmetric_load usually, let's check that too
    print(
        f"Asymmetric Loads: {len(net.asymmetric_load) if 'asymmetric_load' in net else 0}"
    )
    print(f"Trafo1ph: {len(net.trafo1ph) if 'trafo1ph' in net else 0}")

    print("\nSample Bus Data:")
    print(net.bus.head())

    print("\nSample Line Data:")
    print(net.line.head())

except Exception as e:
    print(f"Failed to load network: {e}")
