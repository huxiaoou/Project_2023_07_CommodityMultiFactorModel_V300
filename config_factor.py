"""
created @ 2023-07-31
0.
"""

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
windows_mtm = (10, 21, 63, 126, 189, 252)
factors_mtm = ["MTM"] + [f"MTMS{_:03d}" for _ in windows_mtm]
factors = factors_mtm
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

# secondary parameters
RETURN_SCALE = 100
YIYUAN = 1e8
WANYUAN = 1e4
days_per_year = 252
price_type = "close"

if __name__ == "__main__":
    print("\n".join(factors))
    print("Total number of factors = {}".format(len(factors)))  # 103
