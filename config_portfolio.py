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

# unilateral hold proportion
uni_props = (0.2, 0.3, 0.4)

# secondary parameters
cost_rate_hedge_test = 0e-4
cost_rate_portfolios = 5e-4
cost_reservation = 0e-4
risk_free_rate = 0
top_n = 5
init_premium = 10000 * 1e4
minimum_abs_weight = 0.001

if __name__ == "__main__":
    print("Total number of factors = {}".format(len(factors)))  # 103
    print("\n".join(factors))
