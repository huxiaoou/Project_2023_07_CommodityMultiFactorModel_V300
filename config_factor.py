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

factors_list_size = len(factors)
factors_neutral = ["{}_WS".format(_) for _ in factors]

if __name__ == "__main__":
    print("\n".join(factors))
    print("Total number of factors = {}".format(len(factors)))  # 346
