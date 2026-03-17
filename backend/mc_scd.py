#!/usr/bin/env python
# coding: utf-8

# In[1]:




# In[3]:


import multiconductor as mc
import multiconductor.shortcircuit as sc
from multiconductor.pycci.std_types import create_std_types


def _build_minimal_mc_net():
    net = mc.create_empty_network(sn_mva=1)
    create_std_types(
        net,
        {
            "lv_line": {
                "r_ohm_per_km": 0.1,
                "x_ohm_per_km": 0.2,
                "r0_ohm_per_km": 0.05,
                "x0_ohm_per_km": 0.1,
                "c_nf_per_km": 100,
                "c0_nf_per_km": 50,
                "max_i_ka": 0.2,
            }
        },
        "sequence",
    )

    b0 = mc.create_bus(net, 12.47)
    b1 = mc.create_bus(net, 0.48)

    mc.create_ext_grid(
        net,
        bus=b0,
        from_phase=range(1, 4),
        to_phase=0,
        vm_pu=1.0,
        va_degree=0.0,
        r_ohm=0.01,
        x_ohm=0.1,
        name="slack",
    )
    net.ext_grid["s_sc_max_mva"] = 1000.0
    net.ext_grid["rx_max"] = 0.1
    net.ext_grid["x0x_max"] = 1.0
    net.ext_grid["r0x0_max"] = 0.1
    mc.create_line(
        net,
        std_type="lv_line",
        model_type="sequence",
        from_bus=b0,
        from_phase=range(1, 4),
        to_bus=b1,
        to_phase=range(1, 4),
        length_km=0.1,
        name="line1",
    )
    mc.create_asymmetric_load(
        net,
        b1,
        from_phase=range(1, 4),
        to_phase=0,
        p_mw=(0.01, 0.01, 0.01),
        q_mvar=(0.005, 0.005, 0.005),
    )
    return net



net = _build_minimal_mc_net()
mc.run_pf(net, tol_vmag_pu=1e-9, tol_vang_rad=1e-9)
print("res_bus" in net and not net.res_bus.empty)

sc.calc_sc(net, case="max", fault="LLL", branch_results=False)
print("res_bus_sc" in net and not net.res_bus_sc.empty)


net.res_bus_sc


# In[1]:


import pickle

with open("/Users/admin/git/ppmc4sce/backend/ST_CHARLES.pkl", "rb") as f:
    net = pickle.load(f)

net    


# In[4]:


mc.run_pf(net, tol_vmag_pu=1e-9, tol_vang_rad=1e-9)
net.res_bus


# In[5]:


net.ext_grid["s_sc_max_mva"] = 1000.0
net.ext_grid["rx_max"] = 0.1
net.ext_grid["x0x_max"] = 1.0
net.ext_grid["r0x0_max"] = 0.1

print("bus rows:", len(net.bus))
print("ext_grid rows:", len(net.ext_grid))
print("ext_grid_sequence rows:", len(net.ext_grid_sequence))
print("res_bus_sc shape:", getattr(net, "res_bus_sc", None).shape if hasattr(net, "res_bus_sc") else None)
print("ext_grid columns:", net.ext_grid.columns.tolist())



sc.calc_sc(net, case="min", fault="1ph", branch_results=False)
net.res_bus_sc


# In[6]:


sc.calc_sc(net)
net.res_bus_sc


# In[2]:


from multiconductor.pycci.cci_ica import run_ica_iterative, run_ica_streamlined

ica_df = run_ica_streamlined(net)
ica_df


# In[ ]:


run_ica_iterative(net)


# In[ ]:




# In[8]:


import pandas as pd

walker = pyg.walk(ica_df)

