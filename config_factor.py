import itertools

# --- factor settings ---
wins_aver = (10, 21, 63, 126, 189, 252)
wins_break = (10, 21, 63, 126, 189, 252)
wins_lag = (21, 63, 252)
wins_full_term = (10, 21, 63, 126, 189, 252)
wins_quad_term = (63, 126, 189, 252)
wins_long_term = (126, 252, 378, 504)
factors_settings = {
    "MTM": {"S": wins_aver, "SP": wins_aver},

    "SIZE": {"A": wins_aver, "BR": wins_break, "LR": wins_lag},
    "OI": {"BR": wins_break, "LR": wins_lag},
    "RS": {"BR": wins_break, "LR": wins_lag},

    "BASIS": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "TS": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "LIQUID": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "SR": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "HR": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "NETOI": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "NETOIW": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "NETDOI": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},
    "NETDOIW": {"A": wins_aver, "BD": wins_break, "LD": wins_lag},

    "SKEW": {"": wins_full_term, "LD": wins_lag},
    "VOL": {"": wins_full_term, "LD": wins_lag},
    "RVOL": {"": wins_full_term, "LD": wins_lag},
    "CV": {"": wins_full_term, "LD": wins_lag},

    "CTP": {"": wins_quad_term, "LD": wins_lag},
    "CVP": {"": wins_quad_term, "LD": wins_lag},
    "CSP": {"": wins_quad_term, "LD": wins_lag},
    "BETA": {"": (21,) + wins_quad_term, "LD": wins_lag},

    "VAL": {"": wins_long_term, "LD": wins_lag},
    "CBETA": {"": wins_long_term, "LD": wins_lag},
    "IBETA": {"": wins_long_term, "LD": wins_lag},

    "MACD": {"F": (10,), "S": (21,), "ALPHA": (0.2,)},
    "KDJ": {"N": (10, 20)},
    "RSI": {"N": (10, 20)},
}

factors_mtm = ["MTM"] + list(itertools.chain(*[[f"MTM{k}{_:03d}" for _ in v] for k, v in factors_settings["MTM"].items()]))
factors_size = ["SIZE"] + list(itertools.chain(*[[f"SIZE{k}{_:03d}" for _ in v] for k, v in factors_settings["SIZE"].items()]))
factors_oi = ["OI"] + list(itertools.chain(*[[f"OI{k}{_:03d}" for _ in v] for k, v in factors_settings["OI"].items()]))
factors_rs = ["RS"] + list(itertools.chain(*[[f"RS{k}{_:03d}" for _ in v] for k, v in factors_settings["RS"].items()]))
factors_basis = ["BASIS"] + list(itertools.chain(*[[f"BASIS{k}{_:03d}" for _ in v] for k, v in factors_settings["BASIS"].items()]))
factors_ts = ["TS"] + list(itertools.chain(*[[f"TS{k}{_:03d}" for _ in v] for k, v in factors_settings["TS"].items()]))
factors_liquid = ["LIQUID"] + list(itertools.chain(*[[f"LIQUID{k}{_:03d}" for _ in v] for k, v in factors_settings["LIQUID"].items()]))
factors_sr = ["SR"] + list(itertools.chain(*[[f"SR{k}{_:03d}" for _ in v] for k, v in factors_settings["SR"].items()]))
factors_hr = ["HR"] + list(itertools.chain(*[[f"HR{k}{_:03d}" for _ in v] for k, v in factors_settings["HR"].items()]))
factors_netoi = ["NETOI"] + list(itertools.chain(*[[f"NETOI{k}{_:03d}" for _ in v] for k, v in factors_settings["NETOI"].items()]))
factors_netoiw = ["NETOIW"] + list(itertools.chain(*[[f"NETOIW{k}{_:03d}" for _ in v] for k, v in factors_settings["NETOIW"].items()]))
factors_netdoi = ["NETDOI"] + list(itertools.chain(*[[f"NETDOI{k}{_:03d}" for _ in v] for k, v in factors_settings["NETDOI"].items()]))
factors_netdoiw = ["NETDOIW"] + list(itertools.chain(*[[f"NETDOIW{k}{_:03d}" for _ in v] for k, v in factors_settings["NETDOIW"].items()]))

factors_skew = [f"SKEW{w:03d}" for w in factors_settings["SKEW"][""]] + [f"SKEW{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["SKEW"][""], factors_settings["SKEW"]["LD"])]
factors_vol = [f"VOL{w:03d}" for w in factors_settings["VOL"][""]] + [f"VOL{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["VOL"][""], factors_settings["VOL"]["LD"])]
factors_rvol = [f"RVOL{w:03d}" for w in factors_settings["RVOL"][""]] + [f"RVOL{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["RVOL"][""], factors_settings["RVOL"]["LD"])]
factors_cv = [f"CV{w:03d}" for w in factors_settings["CV"][""]] + [f"CV{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CV"][""], factors_settings["CV"]["LD"])]
factors_ctp = [f"CTP{w:03d}" for w in factors_settings["CTP"][""]] + [f"CTP{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CTP"][""], factors_settings["CTP"]["LD"])]
factors_cvp = [f"CVP{w:03d}" for w in factors_settings["CVP"][""]] + [f"CVP{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CVP"][""], factors_settings["CVP"]["LD"])]
factors_csp = [f"CSP{w:03d}" for w in factors_settings["CSP"][""]] + [f"CSP{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CSP"][""], factors_settings["CSP"]["LD"])]
factors_beta = [f"BETA{w:03d}" for w in factors_settings["BETA"][""]] + [f"BETA{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["BETA"][""], factors_settings["BETA"]["LD"])]
factors_val = [f"VAL{w:03d}" for w in factors_settings["VAL"][""]] + [f"VAL{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["VAL"][""], factors_settings["VAL"]["LD"])]
factors_cbeta = [f"CBETA{w:03d}" for w in factors_settings["CBETA"][""]] + [f"CBETA{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CBETA"][""], factors_settings["CBETA"]["LD"])]
factors_ibeta = [f"IBETA{w:03d}" for w in factors_settings["IBETA"][""]] + [f"IBETA{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["IBETA"][""], factors_settings["IBETA"]["LD"])]

factors_macd = [f"MACDF{f:03d}S{s:03d}A{int(100 * a):03d}" for f, s, a in itertools.product(factors_settings["MACD"]["F"], factors_settings["MACD"]["S"], factors_settings["MACD"]["ALPHA"])]
factors_kdj = [f"KDJ{n:03d}" for n in factors_settings["KDJ"]["N"]]
factors_rsi = [f"RSI{n:03d}" for n in factors_settings["RSI"]["N"]]

factors = factors_mtm + factors_size + factors_oi + factors_rs + \
          factors_basis + factors_ts + factors_liquid + factors_sr + factors_hr + \
          factors_netoi + factors_netoiw + factors_netdoi + factors_netdoiw + \
          factors_skew + factors_vol + factors_rvol + factors_cv + \
          factors_ctp + factors_cvp + factors_csp + factors_beta + \
          factors_val + factors_cbeta + factors_ibeta + \
          factors_macd + factors_kdj + factors_rsi

factors_classification = {
    **{_: "MTM" for _ in factors_mtm},
    **{_: "SIZE" for _ in factors_size},
    **{_: "OI" for _ in factors_oi},
    **{_: "RS" for _ in factors_rs},
    **{_: "BASIS" for _ in factors_basis},
    **{_: "TS" for _ in factors_ts},
    **{_: "LIQUID" for _ in factors_liquid},
    **{_: "SR" for _ in factors_sr},
    **{_: "HR" for _ in factors_hr},
    **{_: "NETOI" for _ in factors_netoi},
    **{_: "NETOIW" for _ in factors_netoiw},
    **{_: "NETDOI" for _ in factors_netdoi},
    **{_: "NETDOIW" for _ in factors_netdoiw},
    **{_: "SKEW" for _ in factors_skew},
    **{_: "VOL" for _ in factors_vol},
    **{_: "RVOL" for _ in factors_rvol},
    **{_: "CV" for _ in factors_cv},
    **{_: "CTP" for _ in factors_ctp},
    **{_: "CVP" for _ in factors_cvp},
    **{_: "CSP" for _ in factors_csp},
    **{_: "BETA" for _ in factors_beta},
    **{_: "VAL" for _ in factors_val},
    **{_: "CBETA" for _ in factors_cbeta},
    **{_: "IBETA" for _ in factors_ibeta},
    **{_: "MACD" for _ in factors_macd},
    **{_: "KDJ" for _ in factors_kdj},
    **{_: "RSI" for _ in factors_rsi},
}

factors_group = {
    "MTM": factors_mtm,
    "SIZE": factors_size,
    "OI": factors_oi,
    "RS": factors_rs,
    "BASIS": factors_basis,
    "TS": factors_ts,
    "LIQUID": factors_liquid,
    "SR": factors_sr,
    "HR": factors_hr,
    "NETOI": factors_netoi,
    "NETOIW": factors_netoiw,
    "NETDOI": factors_netdoi,
    "NETDOIW": factors_netdoiw,
    "SKEW": factors_skew,
    "VOL": factors_vol,
    "RVOL": factors_rvol,
    "CV": factors_cv,
    "CTP": factors_ctp,
    "CVP": factors_cvp,
    "CSP": factors_csp,
    "BETA": factors_beta,
    "VAL": factors_val,
    "CBETA": factors_cbeta,
    "IBETA": factors_ibeta,
    "MACD": factors_macd,
    "KDJ": factors_kdj,
    "RSI": factors_rsi,
}

factors_list_size = len(factors)
factors_neutral = ["{}_WS".format(_) for _ in factors]

if __name__ == "__main__":
    s = 0
    for g, fs in factors_group.items():
        print(f"number of {g:>8s} = {len(fs):>3d}")
        s += len(fs)
    print(f"      number of factors group          = {len(factors_group):>3d}")
    print(f"Sum   number of factors group          = {s:>3d}")
    print(f"Total number of factors classification = {len(factors_classification):>3d}")
    print(f"Total number of factors                = {len(factors):>3d}")
    # the last three number should be 410
