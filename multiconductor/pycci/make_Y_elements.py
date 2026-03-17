from collections import defaultdict
import numbers
import warnings
import numpy as np
import pandas as pd
from scipy import sparse
from numpy.linalg import inv
from numba import njit

from .model import get_bus_terminal, get_line_from_terminal, get_line_to_terminal


@njit(cache=True)
def densefind(A):
    ii,jj=np.nonzero(A)
    vv=[]
    for k in range(ii.shape[0]):
        vv.append(A[ii[k],jj[k]])
    vv=np.array(vv)
    return ii,jj,vv


@njit(cache=True)
def compute_ybr_branch_optimized(Zu, Yu, length, z_base):
    n = Zu.shape[0]

    z_long = Zu * length
    y_tr = Yu * length * z_base
    z_norm = z_long / z_base

    if n == 1:
        y_long = np.zeros((1, 1), dtype=np.complex128)
        y_long[0, 0] = 1.0 / z_norm[0, 0]
    elif n == 2:
        y_long = inverse_2x2(z_norm)
    elif n == 3:
        y_long = inverse_3x3(z_norm)
    else:
        y_long = inv(z_norm)

    Ybr = np.zeros((2*n, 2*n), dtype=np.complex128)
    y_tr_half = y_tr / 2.0

    for i in range(n):
        for j in range(n):
            diag_term = y_long[i, j] + y_tr_half[i, j]
            Ybr[i, j] = diag_term
            Ybr[n+i, n+j] = diag_term
            Ybr[i, n+j] = -y_long[i, j]
            Ybr[n+i, j] = -y_long[i, j]

    return Ybr


@njit(cache=True)
def inverse_2x2(A):
    det = A[0, 0] * A[1, 1] - A[0, 1] * A[1, 0]

    A_inv = np.zeros((2, 2), dtype=np.complex128)
    A_inv[0, 0] = A[1, 1] / det
    A_inv[0, 1] = -A[0, 1] / det
    A_inv[1, 0] = -A[1, 0] / det
    A_inv[1, 1] = A[0, 0] / det

    return A_inv


@njit(cache=True)
def inverse_3x3(A):
    det = (A[0, 0] * (A[1, 1] * A[2, 2] - A[1, 2] * A[2, 1]) -
           A[0, 1] * (A[1, 0] * A[2, 2] - A[1, 2] * A[2, 0]) +
           A[0, 2] * (A[1, 0] * A[2, 1] - A[1, 1] * A[2, 0]))

    A_inv = np.zeros((3, 3), dtype=np.complex128)

    A_inv[0, 0] = (A[1, 1] * A[2, 2] - A[1, 2] * A[2, 1]) / det
    A_inv[0, 1] = (A[0, 2] * A[2, 1] - A[0, 1] * A[2, 2]) / det
    A_inv[0, 2] = (A[0, 1] * A[1, 2] - A[0, 2] * A[1, 1]) / det

    A_inv[1, 0] = (A[1, 2] * A[2, 0] - A[1, 0] * A[2, 2]) / det
    A_inv[1, 1] = (A[0, 0] * A[2, 2] - A[0, 2] * A[2, 0]) / det
    A_inv[1, 2] = (A[0, 2] * A[1, 0] - A[0, 0] * A[1, 2]) / det

    A_inv[2, 0] = (A[1, 0] * A[2, 1] - A[1, 1] * A[2, 0]) / det
    A_inv[2, 1] = (A[0, 1] * A[2, 0] - A[0, 0] * A[2, 1]) / det
    A_inv[2, 2] = (A[0, 0] * A[1, 1] - A[0, 1] * A[1, 0]) / det

    return A_inv


def _make_ynet(net):
    sbase = net.sn_mva*1e6
    model = net["model"]
    terminal_to_y_lookup = net.model.terminal_to_y_lookup
    bt = get_bus_terminal
    lft = get_line_from_terminal
    ltt = get_line_to_terminal

    line_info = {"indices": [],
                 "num_circuits": [],
                 "Yprimitive": [],
                 "from_terminals": [],
                 "to_terminals": [],
                 "y_send_rec": []}

    line = net.line
    oos_lines = {i for (i, _) in net.line.index.values[~line["in_service"].values]}
    olidx = -1
    for (lidx, _), from_bus, from_terminal, to_terminal, model_, std_type, length in zip(line.index.values,
                                                                line["from_bus"].values,
                                                                line["from_phase"].values,
                                                                line["to_phase"].values,
                                                                line["model_type"].values,
                                                                line["std_type"].values,
                                                                line["length_km"].values,
                                                                ):
        if lidx in oos_lines:
            continue
        if lidx == olidx:
            line_info["num_circuits"][-1] += 1
            line_info["from_terminals"][-1].append(from_terminal)
            line_info["to_terminals"][-1].append(to_terminal)
            continue

        olidx = lidx
        line_info["indices"].append(lidx)
        line_info["num_circuits"].append(1)
        line_info["from_terminals"].append([from_terminal])
        line_info["to_terminals"].append([to_terminal])

        z_base = (model.terminal_vn_kv[bt(model, from_bus, 0)] * 1e3 / np.sqrt(3)) ** 2 / sbase
        t = get_c_type(net, model_, std_type)
        Ybr = compute_ybr_branch_optimized(t['Zu'], t['Yu'], length, z_base)
        line_info["Yprimitive"].append(Ybr)

    Y_i = []
    Y_j = []
    Y_v = []

    nlines = len(net.line)

    primpos = 0
    indpos = 0
    nlines = len(net.line)
    Yprimitives = np.empty(shape=(8, nlines * 8), dtype=np.complex128)
    indizes = np.empty(10 * nlines, dtype=np.int64)  # 10 = 1 (size Y) + 1 (line index) + 8 (from and to terminals)

    y_from_to = np.zeros(2 * len(net.line))
    bi = 0
    for lidx, term1, term2, Ybr in zip(line_info["indices"], line_info["from_terminals"], line_info["to_terminals"], line_info["Yprimitive"]):
        y_send = terminal_to_y_lookup[[lft(model, lidx, terminal) for terminal in term1]]
        y_rec = terminal_to_y_lookup[[ltt(model, lidx, terminal)  for terminal in term2]]
        y_send_rec = np.append(y_send, y_rec)

        y_from_to[bi:bi + len(y_send_rec)] = y_send_rec
        bi += len(y_send_rec)
        #TODO performance can be higher when preallocating numpy array for iijjvv
        ii, jj, vv = densefind(Ybr)
        Y_i.extend(y_send_rec[ii].tolist())
        Y_j.extend(y_send_rec[jj].tolist())
        Y_v.extend(vv.tolist())
        line_info["y_send_rec"].append(y_send_rec)

        size = Ybr.shape[0]
        Yprimitives[:size, primpos:primpos+size] = Ybr

        primpos += size
        indizes[indpos] = size
        indizes[indpos + 1] = lidx
        indizes[indpos+2 : indpos+2+size] = y_send_rec
        indpos += 2 + size
    line_info["Yprimitives"] = Yprimitives[:primpos]
    line_info["indizes"] = indizes[:indpos]
    model.line_info = line_info

    model.Y_network = sparse.coo_matrix((Y_v, (Y_i, Y_j)), shape=(model.y_size, model.y_size)).tocsr()

    line_info["y_from_to"] = y_from_to


def _make_yswitch(net):
    net.model.Y_switch = sparse.csr_matrix((net.model.y_size, net.model.y_size))
    switch = net.switch

    if "r_ohm" not in switch:
        return

    sbase = net.sn_mva * 1e6
    model = net.model
    terminal_to_y_lookup = model.terminal_to_y_lookup

    switches = switch[
        (switch['closed'].values) & 
        (switch['et'].values == "b") & 
        (switch['r_ohm'].values > 0)
    ]

    if len(switches) == 0:
        return

    buses = switches['bus'].values
    phase = switches['phase'].values
    element = switches['element'].values

    bterms = terminal_to_y_lookup[np.array([4 * bus + phase for bus, phase in zip(buses, phase)])]
    eterms = terminal_to_y_lookup[np.array([4 * bus + phase for bus, phase in zip(element, phase)])]

    vbase = model.terminal_vn_kv[bterms] * 1e3 / 1.7320508075688772
    z_base = vbase ** 2 / sbase
    Z = switches['r_ohm'].values
    Y_value = z_base / Z

    Y_i = []
    Y_j = []
    Y_v = []
    
    for bterm, eterm, y in zip(bterms, eterms, Y_value):
        Y_i.extend([bterm, eterm])
        Y_j.extend([bterm, eterm])
        Y_v.extend([y, y])
        Y_i.extend([bterm, eterm])
        Y_j.extend([eterm, bterm])
        Y_v.extend([-y, -y])
    
    Y = sparse.coo_matrix((Y_v, (Y_i, Y_j)), shape=(model.y_size, model.y_size))
    net.model.Y_switch = Y.tocsr()


@njit(cache=True)
def collect_Y_ground_numba(cx, cv, cpos, bus_, phase_, r_, x_, gr_, vn_, _is_, terminal_to_y_lookup, sbase):
    for bus, phase, r, x, gr, vn, is_ in zip(bus_, phase_, r_, x_, gr_, vn_, _is_):
        terminal_y = terminal_to_y_lookup[bus * 4 + phase]
        if not gr or not is_ or terminal_y == -1 or (r == 0 and x == 0):
            continue       
        vbase = vn * 1e3 / 1.7320508075688772
        z_base = vbase ** 2 / sbase
        z_ground = r + 1j * x
        cx[cpos] = terminal_y
        cv[cpos] = z_base / z_ground
        cpos += 1
    return cpos


def _make_yground(net):
    sbase = net.sn_mva*1e6
    model = net.model
    terminal_to_y_lookup = net.model.terminal_to_y_lookup
    bustable = net["bus"]

    num_buses = len(bustable)
    cx = np.ones(num_buses, dtype=np.int32) * -10000000
    cv = np.zeros(num_buses, dtype=np.complex128)
    cpos = 0

    cpos = collect_Y_ground_numba(cx, cv, cpos,
                    bustable.index.codes[0],
                    bustable.index.codes[1],
                    bustable["grounding_r_ohm"].values,
                    bustable["grounding_x_ohm"].values,
                    bustable["grounded"].values,
                    bustable["vn_kv"].values,
                    bustable["in_service"].values,
                    terminal_to_y_lookup,
                    sbase)
    Y = sparse.coo_matrix((cv[:cpos], (cx[:cpos], cx[:cpos])),
                          shape=(model.y_size, model.y_size)).tocsr()
    model.Y_ground = Y


def _make_ytrafo(net):
    sbase = net.sn_mva*1e6
    terminal_to_y_lookup = net.model.terminal_to_y_lookup
    terminal_vn_kv = net.model.terminal_vn_kv
    NUM_PHASES = 4
    trafo = net.trafo1ph

    Y_i = []
    Y_j = []
    Y_v = []

    coils = defaultdict(list)
    is_delta = defaultdict(lambda: True)
    for (tidx, bus, circuit), from_phase, to_phase, dU, Tap, vn_kv, pcc, vcc, p0, i0, Pn in zip(trafo.index, trafo['from_phase'].values, trafo['to_phase'].values, trafo['tap_step_percent'].values, trafo['tap_pos'].values, trafo['vn_kv'].values,
                                                trafo['vkr_percent'].values, trafo['vk_percent'].values, trafo['pfe_kw'].values, trafo['i0_percent'].values,
                                                trafo['sn_mva'].values):
        if from_phase == 0 or to_phase == 0:
            is_delta[bus] = False
        Pn *= 1e6
        VBn = terminal_vn_kv[bus * NUM_PHASES + from_phase] / np.sqrt(3) * 1e3
        KOLTC = 1 + 1/100. * dU * Tap
        VRn = vn_kv * 1000. * KOLTC  #[V]
        ZCC_abs = (vcc / Pn) * sbase / 100.
        RCC = (pcc / Pn) * sbase / 100.
        XCC = np.sqrt(ZCC_abs**2 - RCC**2)
        zt = RCC + 1j * XCC
        # Iron-core losses:
        y0_abs = i0 * Pn / sbase / 100
        g0 = p0 / sbase * 1000
        o = y0_abs**2 - g0**2
        if o < 0:
            warnings.warn(f"Transformer (index: {tidx}) shunt parameter incorrect - check pfe_kw and io_percent")
            o = 0
        y0 = g0 - 1j * np.sqrt(o)
        coils[(tidx, circuit)].append({"bus": bus,
                                       "zt": zt,
                                       "y0": y0,
                                       "rapp":  VBn / VRn,
                                       "Pn": Pn,
                                       "is_p2p": from_phase != 0 and to_phase != 0,
                                       "from_terminal": bus * NUM_PHASES + from_phase,
                                       "to_terminal": bus * NUM_PHASES + to_phase})

    circuits = dict()
    for (tidx, cidx), coils in coils.items():
        nCoil = len(coils)
        M = np.zeros((nCoil, nCoil))
        YP_0 = np.zeros((nCoil + 1, nCoil + 1), dtype='complex')
        C = np.zeros((nCoil, nCoil * 2))
        grid_t = np.zeros((nCoil, 2), dtype=np.int64)
        y0_sum = 0
        buses = []
        is_p2p = []
        pns = []
        antifloat_terminals = []
        for k, coil in enumerate(coils):
            buses.append(coil["bus"])
            is_p2p.append(coil["is_p2p"])
            pns.append(coil["Pn"])
            M[k, k] = coil["rapp"]
            YP_0[k, k] = 1 / coil["zt"]
            y0_sum += coil["y0"]
            C[k, k * 2] = 1
            C[k, k * 2 + 1] = -1
            grid_t[k, 0] = terminal_to_y_lookup[coil["from_terminal"]]
            grid_t[k, 1] = terminal_to_y_lookup[coil["to_terminal"]]

            if is_delta[coil["bus"]]:
                antifloat_terminals.append(k*2)

        if y0_sum == 0:
            yt = 1 / (2 * coil["zt"])
            YW = np.array([[yt, -yt], [-yt, yt]])
        else:
            YP_0[k + 1, k + 1] = y0_sum
            A_0 = np.eye(nCoil + 1)
            A_0[:nCoil, nCoil] = -1
            A = A_0.T @ YP_0 @ A_0
            if A.shape[0] == 3:
                YW = inverse_2x2(inverse_3x3(A)[:nCoil, :nCoil])
            else:
                YW = inv(inv(A)[:nCoil, :nCoil])

        Ybr = C.T @ (M.T @ YW @ M) @ C

        for k in antifloat_terminals:
            Ybr[k, k] += abs(YW[0,0] * 1e-6)              # YW used just as reference of the order of magnitude of the admittances
            Ybr[k + 1, k + 1] += abs(YW[0,0] * 1e-6)

        circuits[(tidx, cidx)] = {"Ybr": Ybr,
                                  "buses": buses,
                                  "pns": pns,
                                  "is_p2p": is_p2p,
                                  "y_send_rec": grid_t.flatten()}  # [side1_from side1_to side2_from side2_to]

        ii, jj, vv = densefind(Ybr)        
        aux = grid_t.flatten()
        Y_i.extend(aux[ii].tolist())
        Y_j.extend(aux[jj].tolist())
        Y_v.extend(vv.tolist())

    net.model.Y_tran = sparse.coo_matrix((Y_v, (Y_i, Y_j)), shape=(net.model.y_size, net.model.y_size)).tocsr()
    net.model.trafo_circuits = circuits


def _make_yshunt(net):
    if "asymmetric_shunt" not in net or len(net.asymmetric_shunt) == 0:
        net.model.Y_shunt = sparse.csr_matrix((net.model.y_size, net.model.y_size))
        return

    shunts = net.asymmetric_shunt
    terminal_to_y_lookup = net.model.terminal_to_y_lookup

    # to do -> check if Y_shunt is correct for multiple shunts with same connection 
    active_shunts = shunts[shunts['in_service'] & shunts['closed']]
    num_active_shunts = len(active_shunts)
    if num_active_shunts == 0:
        net.model.Y_shunt = sparse.csr_matrix((net.model.y_size, net.model.y_size))
        return

    Y_i = np.empty(num_active_shunts, dtype=int)
    Y_j = np.empty(num_active_shunts, dtype=int)
    Y_v = np.empty(num_active_shunts, dtype=complex)

    buses = active_shunts['bus'].values
    phases = active_shunts['from_phase'].values
    q_mvars = active_shunts['q_mvar'].values
    p_mws = active_shunts['p_mw'].values

    terms = np.array([get_bus_terminal(net.model, bus, phase) for bus, phase in zip(buses, phases)])
    yterms = terminal_to_y_lookup[terms]

    Y_shunt_values =  (p_mws + 1j * q_mvars) / net.sn_mva
    
    Y_i[:] = yterms
    Y_j[:] = yterms
    Y_v[:] = Y_shunt_values

    Y = sparse.coo_matrix((Y_v, (Y_i, Y_j)), shape=(net.model.y_size, net.model.y_size)).tocsr()
    net.model.Y_shunt = Y


def _make_ysource(net):
    if net.model.is_ideal_ext_grid:
        net.model.Y_source = sparse.csr_matrix((net.model.y_size, net.model.y_size))
        return
    model = net.model
    sbase = net.sn_mva * 1e6
    terminal_to_y_lookup = model.terminal_to_y_lookup
    source = net.ext_grid
    source_sq = net.ext_grid_sequence
    
    Zbase = (model.terminal_vn_kv[model.terminal_is_slack][0] * 1e3 / np.sqrt(3)) ** 2 / sbase
    
    if source_sq.shape[0] > 0:
        alpha = np.exp(1j * np.pi * 2/3)    
        T = np.array([[1, 1 ,1],
                      [1, alpha**2, alpha],
                      [1, alpha, alpha**2]])
        Ngrid = source_sq['bus'].values[0]
        Nph = source_sq['from_phase']
        Zdata = source_sq['r_ohm'].values + 1j * source_sq['x_ohm'].values
        Ysq = inv(np.diag(Zdata))
        Yph = T @ Ysq @ inv(T)

    if source.shape[0]>0:
        Ngrid = source['bus'].values[0]
        Nph = source['from_phase']
        Zdata = source['r_ohm'].values + 1j*source['x_ohm'].values
        Yph = inv(np.diag(Zdata))

    lb = get_bus_terminal
    grid_terms = np.array([terminal_to_y_lookup[lb(net.model, Ngrid, phase)] for phase in Nph])
    source_terms = terminal_to_y_lookup[model.ext_grid_index_start:model.ext_grid_index_start + 3]

    # Treat the internal impedance of the source as a branch:
    Ysource = np.block([[Yph, -Yph], [-Yph, Yph]]) * Zbase
    ii, jj, vv = densefind(Ysource)
    aux = np.append(grid_terms, source_terms)
    Y_i = aux[ii].tolist()
    Y_j = aux[jj].tolist()
    Y_v = vv.tolist()
    Y = sparse.coo_matrix((Y_v, (Y_i, Y_j)), shape=(net.model.y_size, net.model.y_size)).tocsr()
    net.model.Y_source = Y


def get_c_type(net, model, std_type):
    if (model, std_type) in net.model.line_std_types:
        return net.model.line_std_types[(model, std_type)]
    if model == "matrix":
        t = net.std_types[model][std_type]
        z = {}
        for w in "rxgb":
             z[w] = np.array([o for i in range(1, 5) if (o := t.get(f"{w}_{i}_{'ohm' if w in 'rx' else 'us'}_per_km", None)) is not None])
        c = {"Zu": z["r"] + 1j*z["x"],
             "Yu": (z["g"] + 1j*z["b"]) * 1e-6,
             "Imax": np.array(t["max_i_ka"])}
    else:
        w = net.f_hz * 2 * np.pi
        alpha = np.exp(1j * np.pi * 2 / 3)
        T =  np.array([[1, 1, 1], [1, alpha ** 2, alpha], [1, alpha, alpha ** 2]])
        CFG = net.std_types['sequence'][std_type]

        r1 = CFG['r_ohm_per_km']
        x1 = CFG['x_ohm_per_km']
        c1 = CFG['c_nf_per_km']
        r0 = CFG['r0_ohm_per_km']
        x0 = CFG['x0_ohm_per_km']
        c0 = CFG['c0_nf_per_km']
        I = CFG['max_i_ka'] * np.ones(3)

        Z1 = r1 + x1 * 1j
        Z0 = r0 + x0 * 1j

        Y1 = c1 * 1e-9 * 1j * w
        Y0 = c0 * 1e-9 * 1j * w

        ZSQ = np.diag([Z0, Z1, Z1])
        YSQ = np.diag([Y0, Y1, Y1])

        tinv =  inv(T)
        c = {"Zu": T @ ZSQ @ tinv,
             "Yu": T @ YSQ @ tinv,
             "Imax": CFG['max_i_ka'] * np.ones(3)}

    net.model.line_std_types[(model, std_type)] = c
    return c