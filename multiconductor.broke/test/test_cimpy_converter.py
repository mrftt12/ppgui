from multiconductor.cimpy_converter import cim_to_multiconductor, multiconductor_to_cim
from multiconductor.create import create_asymmetric_load, create_bus, create_ext_grid, create_line
from multiconductor.file_io import create_empty_network
from multiconductor.pycci.std_types import create_std_type


def _build_network():
    net = create_empty_network(name="cim_test")
    b0 = create_bus(net, vn_kv=20.0, name="Grid", num_phases=4)
    b1 = create_bus(net, vn_kv=20.0, name="Load", num_phases=4)

    create_ext_grid(
        net,
        bus=b0,
        from_phase=[1, 2, 3],
        to_phase=[0, 0, 0],
        vm_pu=1.0,
        va_degree=0.0,
        r_ohm=0.0,
        x_ohm=0.1,
        name="Slack",
    )
    create_asymmetric_load(
        net,
        bus=b1,
        from_phase=[1, 2, 3],
        to_phase=[0, 0, 0],
        p_mw=[0.1, 0.1, 0.1],
        q_mvar=[0.02, 0.02, 0.02],
        name="L1",
    )

    create_std_type(
        net,
        {
            "r_ohm_per_km": 0.1,
            "x_ohm_per_km": 0.08,
            "r0_ohm_per_km": 0.3,
            "x0_ohm_per_km": 0.24,
            "c_nf_per_km": 10.0,
            "c0_nf_per_km": 30.0,
            "max_i_ka": 1.0,
        },
        name="test_seq",
        element="sequence",
        overwrite=True,
        check_required=False,
    )
    create_line(
        net,
        std_type="test_seq",
        model_type="sequence",
        from_bus=b0,
        from_phase=[1, 2, 3],
        to_bus=b1,
        to_phase=[1, 2, 3],
        length_km=1.2,
        name="Line 1",
    )
    return net


def test_multiconductor_to_cim_has_expected_sections():
    net = _build_network()

    cim = multiconductor_to_cim(net)

    assert "TopologicalNode" in cim
    assert "ExternalNetworkInjection" in cim
    assert "EnergyConsumer" in cim
    assert "ACLineSegment" in cim
    assert len(cim["TopologicalNode"]) == 2
    assert len(cim["ExternalNetworkInjection"]) == 1
    assert len(cim["EnergyConsumer"]) == 1
    assert len(cim["ACLineSegment"]) == 1


def test_cim_round_trip_to_multiconductor():
    net = _build_network()
    cim = multiconductor_to_cim(net)

    imported = cim_to_multiconductor(cim)

    assert len(imported.bus.index.get_level_values(0).unique()) == 2
    assert len(imported.ext_grid.index.get_level_values(0).unique()) == 1
    assert len(imported.asymmetric_load.index.get_level_values(0).unique()) == 1
    assert len(imported.line.index.get_level_values(0).unique()) == 1
