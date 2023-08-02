import itertools as ittl
from skyrim.falkreath import CLib1Tab1, CTable
from config_project import factors_pool_options, sector_classification, concerned_instruments_universe
from config_project import test_windows, factors_return_lags, neutral_method, sectors
from config_factor import factors, factors_neutral

# --- DATABASE STRUCTURE
# available universe structure
database_structure: dict[str, CLib1Tab1] = {
    "available_universe": CLib1Tab1(
        t_lib_name="available_universe.db",
        t_tab=CTable({
            "table_name": "available_universe",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"return": "REAL", "amount": "REAL"}
        })
    )}

# test return structure
database_structure.update({
    "test_return": CLib1Tab1(
        t_lib_name="test_return.db",
        t_tab=CTable({
            "table_name": "test_return",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )})

# test return neutral structure
database_structure.update({
    "test_return.WS": CLib1Tab1(
        t_lib_name="test_return.WS.db",
        t_tab=CTable({
            "table_name": "test_return",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )})

# factor structure
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in factors})

# factors neutral structure
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z.split(".")[0],
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in factors_neutral})

# norm factors pool
norm_factors_pool_list = ["{}.WS.NORM".format(p) for p in factors_pool_options]
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z.split(".")[0],
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {f: "REAL" for f in factors_pool_options[z.split(".")[0]]},
        })
    ) for z in norm_factors_pool_list})

# delinear factors pool
delinear_factors_pool_list = ["{}.WS.DELINEAR".format(p) for p in factors_pool_options]
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z.split(".")[0],
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {f: "REAL" for f in factors_pool_options[z.split(".")[0]]},
        })
    ) for z in delinear_factors_pool_list})

# factors return lib
factors_return_list = ["factors_return.{}.WS.TW{:03d}.T{}".format(p, tw, l)
                       for p, tw, l in ittl.product(factors_pool_options, test_windows, factors_return_lags)]
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z.split(".")[0],
            "primary_keys": {"trade_date": "TEXT", "factor": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in factors_return_list})

# instrument residual lib
instrument_residual_list = ["instruments_residual.{}.WS.TW{:03d}.T{}".format(p, tw, l)
                            for p, tw, l in ittl.product(factors_pool_options, test_windows, factors_return_lags)]
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z.split(".")[0],
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in instrument_residual_list})

# factors portfolio lib
factors_portfolio_list = ["factors_portfolio.{}.WS.TW{:03d}.T{}".format(p, tw, l)
                          for p, tw, l in ittl.product(factors_pool_options, test_windows, factors_return_lags)]
for z in factors_portfolio_list:
    sector_set = {sector_classification[u] for u in concerned_instruments_universe}  # this set may be a subset of sectors_list and in random order
    selected_sectors_list = [z for z in sectors if z in sector_set]  # sort sector set by sectors list order
    selected_factors_pool = factors_pool_options[z.split(".")[1]]  # selected factors pool
    database_structure.update({
        z: CLib1Tab1(
            t_lib_name=z + ".db",
            t_tab=CTable({
                "table_name": z.split(".")[0],
                "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
                "value_columns": {f: "REAL" for f in ["MARKET"] + selected_sectors_list + selected_factors_pool}
            })
        )})

# ic tests
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "ic_test",
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in [f"ic-{f}-TW{tw:03d}" for f, tw in ittl.product(factors, test_windows)]})

database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "ic_test",
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in [f"ic-{f}-{nm}-TW{tw:03d}" for f, nm, tw in ittl.product(factors, [neutral_method], test_windows)]})

database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "ic_test",
            "primary_keys": {"trade_date": "TEXT", "factor": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in [f"ic-delinear-{p}-{nm}-TW{tw:03d}-T{l}"
                for p, nm, tw, l in ittl.product(factors_pool_options,
                                                 [neutral_method], test_windows, factors_return_lags)]})

if __name__ == "__main__":
    print(norm_factors_pool_list)
    print(database_structure["P3.WS.NORM"].m_tab.m_value_columns)
