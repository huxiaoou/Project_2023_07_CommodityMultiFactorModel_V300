"""
created @ 2023-07-31
0.
"""

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

    "IC": "20150701",  # ic-test
    "ICN": "20150701",  # ic-test
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

# --- test return ---
test_windows = [5, 10, 15, 20]  # 4
test_lag = 1
factors_return_lags = (0, 1)

# --- factors pool ---
factors_pool_options = {
    "P3": ["BASIS147", "CSP189", "CTP063", "CVP063",
           "SKEW010", "MTM231", "TS126", "RSW252HL063",
           "SIZE252", "TO252", "BETA021"],
}
selected_pool = factors_pool_options["P3"]

# neutral methods
neutral_method = "WS"
