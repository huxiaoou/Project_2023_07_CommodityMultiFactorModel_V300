from skyrim.falkreath import CLib1Tab1, CTable
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
            "table_name": z,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in factors_neutral})

# ic tests
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "ic_test",
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in [f"ic-{f}" for f in factors]})

database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "ic_test",
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in [f"ic-{f}" for f in factors_neutral]})

if __name__ == "__main__":
    with open("E:\\tmp\\ds.json", "w+") as f:
        f.write(str(database_structure))
