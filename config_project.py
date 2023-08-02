"""
created @ 2023-07-31
0.
"""

import itertools

#
bgn_dates_in_overwrite_mod = {
    "IR": "20120101",  # instrument_return
    "AU": "20120301",  # available_universe
    "MR": "20120301",  # market_return
    "TR": "20120301",  # test_return
    "TRN": "20120301",  # test_return_neutral

    "FEB": "20120101",  # factor_exposure basic
    "FE": "20130101",  # factor_exposure
    "FEN": "20130101",  # factor_exposure_neutral

    "DLN": "20130201",  # factor_exposure_norm_and_delinear

    "FR": "20140101",  # factor_return
}

# universe
concerned_instruments_universe = [
    "AU.SHF",
    "AG.SHF",
    "CU.SHF",
    "AL.SHF",
    "PB.SHF",
    "ZN.SHF",
    "SN.SHF",
    "NI.SHF",
    "SS.SHF",
    "RB.SHF",
    "HC.SHF",
    "J.DCE",
    "JM.DCE",
    "I.DCE",
    "FG.CZC",
    "SA.CZC",
    "UR.CZC",
    "ZC.CZC",
    "SF.CZC",
    "SM.CZC",
    "Y.DCE",
    "P.DCE",
    "OI.CZC",
    "M.DCE",
    "RM.CZC",
    "A.DCE",
    "RU.SHF",
    "BU.SHF",
    "FU.SHF",
    "L.DCE",
    "V.DCE",
    "PP.DCE",
    "EG.DCE",
    "EB.DCE",
    "PG.DCE",
    "TA.CZC",
    "MA.CZC",
    "SP.SHF",
    "CF.CZC",
    "CY.CZC",
    "SR.CZC",
    "C.DCE",
    "CS.DCE",
    "JD.DCE",
    "LH.DCE",
    "AP.CZC",
    "CJ.CZC",
]
ciu_size = len(concerned_instruments_universe)  # should be 47

# available universe
available_universe_options = {
    "rolling_window": 20,
    "amount_threshold": 5,
}

# sector
sectors = ["AUAG", "METAL", "BLACK", "OIL", "CHEM", "MISC"]  # 6
sector_classification = {
    "AU.SHF": "AUAG",
    "AG.SHF": "AUAG",
    "CU.SHF": "METAL",
    "AL.SHF": "METAL",
    "PB.SHF": "METAL",
    "ZN.SHF": "METAL",
    "SN.SHF": "METAL",
    "NI.SHF": "METAL",
    "SS.SHF": "METAL",
    "RB.SHF": "BLACK",
    "HC.SHF": "BLACK",
    "J.DCE": "BLACK",
    "JM.DCE": "BLACK",
    "I.DCE": "BLACK",
    "FG.CZC": "BLACK",
    "SA.CZC": "BLACK",
    "UR.CZC": "BLACK",
    "ZC.CZC": "BLACK",
    "SF.CZC": "BLACK",
    "SM.CZC": "BLACK",
    "Y.DCE": "OIL",
    "P.DCE": "OIL",
    "OI.CZC": "OIL",
    "M.DCE": "OIL",
    "RM.CZC": "OIL",
    "A.DCE": "OIL",
    "RU.SHF": "CHEM",
    "BU.SHF": "CHEM",
    "FU.SHF": "CHEM",
    "L.DCE": "CHEM",
    "V.DCE": "CHEM",
    "PP.DCE": "CHEM",
    "EG.DCE": "CHEM",
    "EB.DCE": "CHEM",
    "PG.DCE": "CHEM",
    "TA.CZC": "CHEM",
    "MA.CZC": "CHEM",
    "SP.SHF": "CHEM",
    "CF.CZC": "MISC",
    "CY.CZC": "MISC",
    "SR.CZC": "MISC",
    "C.DCE": "MISC",
    "CS.DCE": "MISC",
    "LH.DCE": "MISC",
    "JD.DCE": "MISC",
    "AP.CZC": "MISC",
    "CJ.CZC": "MISC",
}

# --- factor settings ---
wins_aver = (10, 21, 63, 126, 189, 252)
wins_break = (10, 21, 63, 126, 189, 252)
wins_lag = (21, 63, 252)
wins_full_term = (10, 21, 63, 126, 189, 252)
wins_quad_term = (63, 126, 189, 252)
wins_long_term = (63, 126, 252, 504)
factors_settings = {
    "MTM": {
        "S": wins_aver, "A": (), "SP": wins_aver, "BD": (), "LD": (), "BR": (), "LR": ()},
    "RS": {
        "S": (), "A": (), "SP": (), "BD": (), "LD": (), "BR": wins_break, "LR": wins_lag},

    "BASIS": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},
    "TS": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},
    "LIQUID": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},
    "SR": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},
    "HR": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},
    "NETOI": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},
    "NETOIW": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},
    "NETDOI": {
        "S": (), "A": wins_aver, "SP": (), "BD": wins_break, "LD": wins_lag, "BR": (), "LR": ()},

    "SKEW": {
        "": wins_full_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "VOL": {
        "": wins_full_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "CV": {
        "": wins_full_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "RSVOL": {
        "": wins_full_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "OI": {
        "": wins_full_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "SIZE": {
        "": wins_full_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},

    "CTP": {
        "": wins_quad_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "CVP": {
        "": wins_quad_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "CSP": {
        "": wins_quad_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "BETA": {
        "": wins_quad_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},

    "VAL": {
        "": wins_long_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "CBETA": {
        "": wins_long_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},
    "IBETA": {
        "": wins_long_term, "S": (), "A": (), "SP": (), "BD": (), "LD": wins_lag, "BR": (), "LR": ()},

    "MACD": {"F": (10,), "S": (21,), "ALPHA": (0.2,)},
    "KDJ": {"N": (10,)},
    "RSI": {"N": (10,)},
}

factors_mtm = ["MTM"] + list(itertools.chain(*[[f"MTM{k}{_:03d}" for _ in v] for k, v in factors_settings["MTM"].items()]))
factors_rs = ["RS"] + list(itertools.chain(*[[f"RS{k}{_:03d}" for _ in v] for k, v in factors_settings["RS"].items()]))
factors_basis = ["BASIS"] + list(itertools.chain(*[[f"BASIS{k}{_:03d}" for _ in v] for k, v in factors_settings["BASIS"].items()]))
factors_ts = ["TS"] + list(itertools.chain(*[[f"TS{k}{_:03d}" for _ in v] for k, v in factors_settings["TS"].items()]))
factors_liquid = ["LIQUID"] + list(itertools.chain(*[[f"LIQUID{k}{_:03d}" for _ in v] for k, v in factors_settings["LIQUID"].items()]))
factors_sr = ["SR"] + list(itertools.chain(*[[f"SR{k}{_:03d}" for _ in v] for k, v in factors_settings["SR"].items()]))
factors_hr = ["HR"] + list(itertools.chain(*[[f"HR{k}{_:03d}" for _ in v] for k, v in factors_settings["HR"].items()]))
factors_netoi = ["NETOI"] + list(itertools.chain(*[[f"NETOI{k}{_:03d}" for _ in v] for k, v in factors_settings["NETOI"].items()]))
factors_netoiw = ["NETOIW"] + list(itertools.chain(*[[f"NETOIW{k}{_:03d}" for _ in v] for k, v in factors_settings["NETOIW"].items()]))
factors_netdoi = ["NETDOI"] + list(itertools.chain(*[[f"NETDOI{k}{_:03d}" for _ in v] for k, v in factors_settings["NETDOI"].items()]))
factors_skew = [f"SKEW{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["SKEW"][""], factors_settings["SKEW"]["LD"])]
factors_vol = [f"VOL{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["VOL"][""], factors_settings["VOL"]["LD"])]
factors_cv = [f"CV{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CV"][""], factors_settings["CV"]["LD"])]
factors_rsvol = [f"RSVOL{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["RSVOL"][""], factors_settings["RSVOL"]["LD"])]
factors_oi = [f"OI{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["OI"][""], factors_settings["OI"]["LD"])]
factors_size = [f"SIZE{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["SIZE"][""], factors_settings["SIZE"]["LD"])]
factors_ctp = [f"CTP{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CTP"][""], factors_settings["CTP"]["LD"])]
factors_cvp = [f"CVP{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CVP"][""], factors_settings["CVP"]["LD"])]
factors_csp = [f"CSP{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CSP"][""], factors_settings["CSP"]["LD"])]
factors_beta = [f"BETA{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["BETA"][""], factors_settings["BETA"]["LD"])]
factors_val = [f"VAL{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["VAL"][""], factors_settings["VAL"]["LD"])]
factors_cbeta = [f"CBETA{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["CBETA"][""], factors_settings["CBETA"]["LD"])]
factors_ibeta = [f"IBETA{w:03d}LD{l:03d}" for w, l in itertools.product(factors_settings["IBETA"][""], factors_settings["IBETA"]["LD"])]
factors_macd = [f"MACDF{f:03d}S{s:03d}A{int(100 * a):03d}" for f, s, a in itertools.product(factors_settings["MACD"]["F"], factors_settings["MACD"]["S"], factors_settings["MACD"]["ALPHA"])]
factors_kdj = [f"KDJ{n:03d}" for n in factors_settings["KDJ"]["N"]]
factors_rsi = [f"RSI{n:03d}" for n in factors_settings["RSI"]["N"]]

factors = factors_mtm + factors_rs + \
          factors_basis + factors_ts + factors_liquid + factors_sr + factors_hr + factors_netoi + factors_netoiw + factors_netdoi + \
          factors_skew + factors_vol + factors_cv + factors_rsvol + factors_oi + factors_size + \
          factors_ctp + factors_cvp + factors_csp + factors_beta + \
          factors_val + factors_cbeta + factors_ibeta + \
          factors_macd + factors_kdj + factors_rsi

factors_list_size = len(factors)
factors_neutral = ["{}.WS".format(_) for _ in factors]

# --- test return ---
test_windows = [3, 5, 10, 15, 20]  # 5
test_lag = 1
factors_return_lags = (0, 1)

# --- factors pool ---
factors_pool_options = {
    "P3": ["BASIS147", "CSP189", "CTP063", "CVP063",
           "SKEW010", "MTM231", "TS126", "RSW252HL063",
           "SIZE252", "TO252", "BETA021"],
}

# neutral methods
neutral_method = "WS"

if __name__ == "__main__":
    print("\n".join(factors))
    print("Total number of factors = {}".format(len(factors)))  # 103
