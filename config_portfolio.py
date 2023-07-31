import itertools as ittl
from config_factor import test_windows
from config_factor import sectors, sector_classification
from config_factor import factors_pool_options, factors
from config_factor import concerned_instruments_universe

# secondary parameters
cost_rate = 5e-4
cost_reservation = 0e-4
risk_free_rate = 0
top_n = 5
init_premium = 10000 * 1e4
minimum_abs_weight = 0.001

# Local
pid = "P3"
factors_return_lag = 0  # the core difference between "Project_2022_11_Commodity_Factors_Return_Analysis_V4B"
selected_sectors = [z for z in sectors if z in set(sector_classification[i] for i in concerned_instruments_universe)]
selected_factors = factors_pool_options[pid]
available_factors = ["MARKET"] + selected_sectors + selected_factors
timing_factors = ["MARKET"] + selected_sectors

fast_n_slow_n_comb = (
    (5, 63),
    (5, 126),
    (21, 126),
    (21, 189),
)

FAR_END_DATE_IN_THE_FUTURE = "20330101"
pure_portfolio_options = {
    # Pure Factors: Long Term
    "A1": {
        FAR_END_DATE_IN_THE_FUTURE: {},
        "20230607": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "BETA021": "pure_factors_VANILLA.BETA021.TW{:03d}".format(20),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(20),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(20),
        },
        "20221201": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(20),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(20),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(20),
        },
        "20170101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(20),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(20),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(20),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(20),
        },
        "20120101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(20),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(20),
        },
    },

    # Pure Factors: Short Term
    "A6": {
        FAR_END_DATE_IN_THE_FUTURE: {},
        "20230607": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "BETA021": "pure_factors_VANILLA.BETA021.TW{:03d}".format(5),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(5),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(5),
        },
        "20221201": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(5),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(5),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(5),
        },
        "20170101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(5),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(5),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(5),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(5),
        },
        "20120101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(5),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(5),
        },
    },

    # Pure Factors: Long Term + SectorTiming
    "A3": {  # A3 = A1 + SectorTiming
        FAR_END_DATE_IN_THE_FUTURE: {},
        "20230607": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "BETA021": "pure_factors_VANILLA.BETA021.TW{:03d}".format(20),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(20),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(20),

            "BLACK": "pure_factors_MA.BLACK.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 189),
            "OIL": "pure_factors_MA.OIL.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(10, 21, 126),
        },
        "20221201": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(20),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(20),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(20),

            "BLACK": "pure_factors_MA.BLACK.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 189),
            "OIL": "pure_factors_MA.OIL.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(10, 21, 126),
        },
        "20170101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(20),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(20),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(20),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(20),

            "BLACK": "pure_factors_MA.BLACK.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 189),
            "OIL": "pure_factors_MA.OIL.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(10, 21, 126),
        },
        "20120101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(20),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(20),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(20),

            "BLACK": "pure_factors_MA.BLACK.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 189),
            "OIL": "pure_factors_MA.OIL.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(15, 21, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(10, 21, 126),
        },

    },

    # Pure Factors: Short Term + SectorTiming
    "A8": {  # A8 =  A6 + SectorTiming
        FAR_END_DATE_IN_THE_FUTURE: {},
        "20230607": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "BETA021": "pure_factors_VANILLA.BETA021.TW{:03d}".format(5),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(5),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(5),

            "MARKET": "pure_factors_MA.MARKET.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
        },
        "20221201": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(5),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(5),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(5),

            "MARKET": "pure_factors_MA.MARKET.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
        },
        "20170101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(5),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(5),
            "SKEW010": "pure_factors_VANILLA.SKEW010.TW{:03d}".format(5),
            "RSW252HL063": "pure_factors_VANILLA.RSW252HL063.TW{:03d}".format(5),

            "MARKET": "pure_factors_MA.MARKET.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
        },
        "20120101": {
            "BASIS147": "pure_factors_VANILLA.BASIS147.TW{:03d}".format(5),
            "CTP063": "pure_factors_VANILLA.CTP063.TW{:03d}".format(5),
            "CSP189": "pure_factors_VANILLA.CSP189.TW{:03d}".format(5),

            "MARKET": "pure_factors_MA.MARKET.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "CHEM": "pure_factors_MA.CHEM.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
            "MISC": "pure_factors_MA.MISC.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(5, 5, 126),
        },
    },
}

raw_portfolio_options = {
    "R1": {
        "RSW252HL063": {"weight": 1 / 6, "SHP": 0.3},
        "BASIS147": {"weight": 1 / 6, "SHP": 0.3},
        "CTP063": {"weight": 1 / 6, "SHP": 0.3},
        "CVP063": {"weight": 1 / 6, "SHP": 0.3},
        "TS126": {"weight": 1 / 6, "SHP": 0.3},
        "MTM231": {"weight": 1 / 6, "SHP": 0.3},
    },

    "R4": {
        "RSW252HL063": {"weight": 1 / 8, "SHP": 0.3},
        "BASIS147": {"weight": 1 / 8, "SHP": 0.3},
        "CTP063": {"weight": 1 / 8, "SHP": 0.3},
        "CVP063": {"weight": 1 / 8, "SHP": 0.3},
        "TS126": {"weight": 1 / 8, "SHP": 0.3},
        "MTM231": {"weight": 1 / 8, "SHP": 0.3},
        "SKEW010": {"weight": 1 / 8, "SHP": 0.3},
        "BETA021": {"weight": 1 / 8, "SHP": 0.3},
    },
}

test_signals = {
    "vanilla": ["{}VM{:03d}".format(factor_lbl, mov_ave_len)
                for factor_lbl, mov_ave_len in ittl.product(available_factors, test_windows)],
    "ma": ["{}F{:03d}S{:03d}M{:03d}".format(factor_lbl, fn, sn, mov_ave_len)
           for factor_lbl, mov_ave_len, (fn, sn) in
           ittl.product(timing_factors, test_windows, fast_n_slow_n_comb)],
    "allocation": ["{}M{:03d}".format(aid, tw) for aid, tw in ittl.product(
        list(raw_portfolio_options) + list(pure_portfolio_options), test_windows)]
}

if __name__ == "__main__":
    import pandas as pd
    print("Total number of factors = {}".format(len(factors)))  # 103
    print("\n".join(factors))
    print(selected_sectors)
    print(selected_factors)
    print(available_factors)
    print("Sectors N:{:>2d}".format(len(selected_sectors)))
    print("Factors N:{:>2d}".format(len(selected_factors)))
    print("ALL     N:{:>2d}".format(len(available_factors)))

    comp_struct = {}
    for k, v in pure_portfolio_options.items():
        comp_struct[k] = {z: 1 for z in v}
    comp_df = pd.DataFrame(comp_struct)
    print(comp_df)
