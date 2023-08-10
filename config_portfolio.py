from config_factor import factors

# secondary parameters
cost_rate = 5e-4
cost_reservation = 0e-4
risk_free_rate = 0
top_n = 5
init_premium = 10000 * 1e4
minimum_abs_weight = 0.001


if __name__ == "__main__":
    print("Total number of factors = {}".format(len(factors)))  # 103
    print("\n".join(factors))

