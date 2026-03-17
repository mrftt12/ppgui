import pytest
import os
import tempfile

import pandapower as pp
import multiconductor as mc
from multiconductor.pycci import create_std_types

def _roundtrip(net, format):
    if format == "xlsx":
        dir = tempfile.gettempdir()
        filepath = os.path.join(dir, "test_mc_file.xlsx")
        pp.to_excel(net, filepath)
        return mc.from_excel(filepath)
    elif format == "json":
        return pp.from_json_string(pp.to_json(net), empty_dict_like_object=mc.create_empty_network())
    else:
        raise NotImplementedError("supported file formats: xlsx, json, received unknown file format", format)


def _check_roundtrip(net, format):
    assert pp.nets_equal(net, _roundtrip(net, format))


def simple_test_net():
    net = mc.create_empty_network()
    mc.create_bus(net, 20, name="test")
    mc.create_bus(net, 0.4, name="test2")
    mc.create_bus(net, 0.4, name="test3")

    mc.create_transformer1ph(net, (0, 1), [1,2,3,1,2,3], [2,3,1,0,0,0], (20, 0.4), 5, 0.01, 0.002, 0.3,
                        5e-4, 0, 10, -10, 0, 0.625, (0, 4), name="Trafo_MV/LV")

    create_std_types(net, {"lv_line": {"r_ohm_per_km": 0.1, "x_ohm_per_km": 0.2, "r0_ohm_per_km": 0.05, "x0_ohm_per_km": 0.1, "c_nf_per_km": 100, "c0_nf_per_km": 50, "max_i_ka": 0.2}}, "sequence")

    mc.create_line(net, std_type="lv_line", model_type="sequence", from_bus=1, from_phase=range(1, 4), to_bus=2,
                    to_phase=range(1, 4), length_km=0.5, name="LV_line_0")

    mc.create_asymmetric_load(net, 2, range(1, 4), 0, 0.01, (0.005, 0.002, -0.007), name="load0")
    mc.create_ext_grid_sequence(net, bus=0, from_phase=range(1, 4), to_phase=0, vm_pu=1, va_degree=0,
                                sn_mva=1000, rx=0.1, x0x=1, r0x0=0.1, name="slack")
    return net


# -> tests with xlsx file need to be reimplemented
# def test_from_excel():
#     filename = os.path.join(mc.mc_dir, "data", "IEEE_13bus_pp.xlsx")
#     net = mc.from_excel(filename)
#     _check_roundtrip(net, "xlsx")


# fails because tuples are not supported as keys of dict for to_json for configuration_std_types - todo fix this
def test_from_json():
    filename = os.path.join(mc.mc_dir, "data", "IEEE_13bus_pp.xlsx")
    net = mc.from_excel(filename)
    _check_roundtrip(net, "json")


#@pytest.mark.parametrize("format", ("xlsx", "json"))
@pytest.mark.parametrize("format", (["json"]))
def test_with_create(format):
    net = simple_test_net()
    _check_roundtrip(net, format)


if __name__ == "__main__":
    pytest.main([__file__])
