from config_project import sectors, sector_classification
from config_project import factors_pool_options
from config_project import concerned_instruments_universe
from config_factor import factors

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

if __name__ == "__main__":
    print("Total number of factors = {}".format(len(factors)))  # 103
    print("\n".join(factors))
    print(selected_sectors)
    print(selected_factors)
    print(available_factors)
    print("Sectors N:{:>2d}".format(len(selected_sectors)))
    print("Factors N:{:>2d}".format(len(selected_factors)))
    print("ALL     N:{:>2d}".format(len(available_factors)))
