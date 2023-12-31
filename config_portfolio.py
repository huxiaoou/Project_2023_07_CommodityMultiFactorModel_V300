from config_factor import factors

selected_raw_factors = (
    "BASISA126",
    "CTP126",
    "RSBR252",
    "SKEW010",
    "SKEW126",
    "CVP189LD021",
    "TSA189",
    "MTMS252",
    "BETA021",
    "VOL021",
    "RSI010",
    "IBETA252LD021",
    "NETDOIBD021",
    "SIZEBR010",
    "RVOL010LD021",
    "HRA063",
    "LIQUIDA189",
    "LIQUIDBD010",
)

selected_neu_factors = (
    "CTP126",
    "SKEW010",
    "MTM",
    "BASISA126",
    "LIQUIDBD010",
    "CSP063",
    "NETDOIWLD252",
    "CVP189LD063",
    "RSBR010",
    "RSLR252",
    "TSA189",
    "NETDOIA252",
    "SIZEA189",
    "SRLD063",
)

selected_raw_factors_and_uni_prop = (
    ("BASISA063", 0.3),
    ("BASISBD126", 0.2),
    ("CSP063", 0.3),
    ("CSP189LD021", 0.4),
    ("CTP126", 0.4),
    ("CTP189LD021", 0.4),
    ("CVP126", 0.3),
    ("CVP189LD021", 0.4),
    ("RSBR189", 0.4),
    ("RSLR252", 0.3),
    ("SKEW010", 0.4),
    ("SKEW010LD063", 0.4),
    ("HRA063", 0.4),
    ("SRLD063", 0.3),
    ("TSA063", 0.4),
    ("TSBD010", 0.4),
    ("LIQUIDBD010", 0.2),
    ("NETDOI", 0.2),
    ("NETDOIBD126", 0.2),
)

selected_neu_factors_and_uni_prop = (
    ("BASISA063", 0.4),
    ("BASISBD010", 0.4),
    ("CSP189", 0.4),
    ("CSP126LD021", 0.4),
    ("CTP126", 0.4),
    ("CTP189LD063", 0.4),
    ("CVP126", 0.2),
    ("CVP189LD063", 0.4),
    ("LIQUIDBD010", 0.3),
    ("NETDOIWA021", 0.3),
    ("RSLR252", 0.3),
    ("SKEW010LD063", 0.4),
)

selected_raw_factors_and_uni_prop_ma = (
    ("BASISA063", 0.3, 10),
    ("CSP063", 0.3, 5),
    ("CSP189LD021", 0.4, 5),
    ("CTP126", 0.3, 5),
    ("CTP189LD021", 0.4, 5),
    ("CVP126", 0.2, 5),
    ("CVP189LD021", 0.4, 15),
    ("RSBR252", 0.4, 5),
    ("RSLR252", 0.3, 5),
    ("SKEW010", 0.2, 5),
    ("SKEW010LD063", 0.4, 5),
    ("HRA063", 0.4, 5),
    ("TSA063", 0.4, 5),
    ("NETDOIBD021", 0.2, 5),
)

selected_neu_factors_and_uni_prop_ma = (
    ("BASISA063", 0.4, 15),
    ("CSP126", 0.4, 5),
    ("CSP189LD021", 0.4, 15),
    ("CTP126", 0.4, 5),
    ("CTP189LD063", 0.4, 5),
    ("CVP126", 0.2, 15),
    ("CVP189LD063", 0.4, 15),
    ("NETDOIWA021", 0.3, 5),
    ("NETDOIWBD189", 0.2, 15),
    ("RSBR252", 0.4, 5),
    ("RSLR252", 0.4, 5),
    ("SKEW010", 0.2, 5),
    ("SKEW010LD063", 0.4, 5),
    ("SKEW189LD021", 0.2, 5),

    # ("BASISA063", 0.2, 5),
    # ("CSP126", 0.2, 5),
    # ("CSP189LD021", 0.2, 5),
    # ("CTP126", 0.2, 5),
    # ("CTP189LD063", 0.2, 5),
    # ("CVP126", 0.2, 5),
    # ("CVP189LD063", 0.2, 5),
    # ("NETDOIWA021", 0.2, 5),
    # ("NETDOIWBD189", 0.2, 5),
    # ("RSBR252", 0.2, 5),
    # ("RSLR252", 0.2, 5),
    # ("SKEW010", 0.2, 5),
    # ("SKEW010LD063", 0.2, 5),
    # ("SKEW189LD021", 0.2, 5),
)

src_signal_ids_raw = [f"{fac}_UHP{int(uhp * 10):02d}" for fac, uhp in selected_raw_factors_and_uni_prop]
src_signal_ids_neu = [f"{fac}_WS_UHP{int(uhp * 10):02d}" for fac, uhp in selected_neu_factors_and_uni_prop]
src_signal_ids_raw_ma = [f"{fac}_UHP{int(uhp * 10):02d}_MA{maw:02d}" for fac, uhp, maw in selected_raw_factors_and_uni_prop_ma]
src_signal_ids_neu_ma = [f"{fac}_WS_UHP{int(uhp * 10):02d}_MA{maw:02d}" for fac, uhp, maw in selected_neu_factors_and_uni_prop_ma]
size_raw, size_raw_ma = len(src_signal_ids_raw), len(src_signal_ids_raw_ma)
size_neu, size_neu_ma = len(src_signal_ids_neu), len(src_signal_ids_neu_ma)

trn_win, lbd = 3, 20  # optimized
# trn_win, lbd = 1, 20  # test
min_model_days = int(trn_win * 21 * 0.9)
test_portfolio_ids = [
    "raw_fix",
    "neu_fix",
    "raw_min_uty_con",
    "neu_min_uty_con",
    "raw_fix_ma",
    "neu_fix_ma",
    "raw_min_uty_con_ma",
    "neu_min_uty_con_ma",
]

# unilateral hold proportion
uni_props = (0.2, 0.3, 0.4)
mov_ave_wins = (5, 10, 15)

# secondary parameters
cost_rate_hedge_test = 0e-4
cost_rate_portfolios = 5e-4
risk_free_rate = 0
performance_indicators = [
    "hold_period_return",
    "annual_return",
    "annual_volatility",
    "sharpe_ratio",
    "calmar_ratio",
    "max_drawdown_scale",
    "max_drawdown_scale_idx",
]

if __name__ == "__main__":
    print("Total number of factors = {}".format(len(factors)))  # 103
    print("\n".join(factors))
