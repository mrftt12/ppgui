import pickle
import os

pkl_path = "networks/ALMOND_12KV.pkl"
with open(pkl_path, "rb") as f:
    net = pickle.load(f)

print("Bus Index Structure:")
print(net.bus.index[:10])
print("\nLine Structure:")
print(net.line.head())
print("\nLine from_bus unique values (first 10):")
print(net.line["from_bus"].unique()[:10])
print("\nLine to_bus unique values (first 10):")
print(net.line["to_bus"].unique()[:10])
