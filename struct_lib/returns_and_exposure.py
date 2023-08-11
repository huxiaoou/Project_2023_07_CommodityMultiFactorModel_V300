from skyrim.falkreath import CLib1Tab1, CTable


def get_lib_struct_available_universe() -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name="available_universe.db",
        t_tab=CTable({
            "table_name": "available_universe",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"return": "REAL", "amount": "REAL"}
        })
    )


# test return structure
def get_lib_struct_test_return() -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name="test_return.db",
        t_tab=CTable({
            "table_name": "test_return",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )


def get_lib_struct_test_return_neutral(neutral_method: str) -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name=f"test_return_{neutral_method}.db",
        t_tab=CTable({
            "table_name": "test_return",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )


def get_lib_struct_factor_exposure(factor: str) -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name=f"{factor}.db",
        t_tab=CTable({
            "table_name": factor,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )


def get_lib_struct_factor_exposure_neutral(factor: str, neutral_method) -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name=f"{factor}_{neutral_method}.db",
        t_tab=CTable({
            "table_name": factor,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )


def get_lib_struct_ic_test(factor: str) -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name=f"ic-{factor}.db",
        t_tab=CTable({
            "table_name": factor,
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )


def get_lib_struct_ic_test_neutral(factor: str, neutral_method: str) -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name=f"ic-{factor}_{neutral_method}.db",
        t_tab=CTable({
            "table_name": factor,
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )
