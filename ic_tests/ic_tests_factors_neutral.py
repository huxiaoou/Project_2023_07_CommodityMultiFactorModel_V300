import os
import itertools
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriter
from skyrim.winterhold import check_and_mkdir
from ic_tests.ic_tests_shared import corr_one_day


def ic_test_neutral(
        factor: str, neutral_method: str, test_window: int,
        run_mode: str, bgn_date: str, stp_date: str,
        ic_tests_dir: str,
        available_universe_dir: str,
        exposure_dir: str,
        return_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
):
    __test_lag = 1

    # --- directory check
    check_and_mkdir(dst_dir := os.path.join(ic_tests_dir, factor))

    # --- set dates
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_bgn_date = calendar.get_next_date(iter_dates[0], -__test_lag - test_window)
    base_stp_date = calendar.get_next_date(iter_dates[-1], -__test_lag - test_window + 1)
    base_dates = calendar.get_iter_list(base_bgn_date, base_stp_date, True)
    bridge_dates_df = pd.DataFrame({"base_date": base_dates, "trade_date": iter_dates})

    # --- available universe
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.set_default(available_universe_lib_structure.m_tab.m_table_name)
    available_universe_df = available_universe_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument"])
    available_universe_lib.close()

    # --- factor library
    factor_lib_structure = database_structure[factor + "." + neutral_method]
    factor_lib = CManagerLibReader(t_db_name=factor_lib_structure.m_lib_name, t_db_save_dir=exposure_dir)
    factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)
    factor_df = factor_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])
    factor_lib.close()

    # --- test return library
    test_return_lib_id = "test_return_{:03d}.{}".format(test_window, neutral_method)
    test_return_lib_structure = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=return_dir)
    test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)
    test_return_df = test_return_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])
    test_return_lib.close()

    factors_exposure_df = pd.merge(
        left=available_universe_df, right=factor_df,
        on=["trade_date", "instrument"], how="inner"
    ).rename(mapper={"trade_date": "base_date"}, axis=1)
    test_return_df_expand = pd.merge(
        left=bridge_dates_df, right=test_return_df,
        on="trade_date", how="right",
    )
    test_input_df = pd.merge(
        left=factors_exposure_df, right=test_return_df_expand,
        on=["base_date", "instrument"], how="inner", suffixes=("_e", "_r")
    )

    res_srs = test_input_df.groupby(by="trade_date", group_keys=True).apply(corr_one_day, x="value_e", y="value_r", method="spearman")
    update_df = pd.DataFrame({"value": res_srs})
    ic_test_lib_id = f"ic-{factor}-{neutral_method}-TW{test_window:03d}"
    ic_test_lib_structure = database_structure[ic_test_lib_id]
    ic_test_lib = CManagerLibWriter(t_db_name=ic_test_lib_structure.m_lib_name, t_db_save_dir=dst_dir)
    ic_test_lib.initialize_table(t_table=ic_test_lib_structure.m_tab, t_remove_existence=run_mode in ["O"])
    ic_test_lib.update(t_update_df=update_df, t_using_index=True)
    ic_test_lib.close()
    return 0


def cal_ic_tests_neutral_mp(
        proc_num: int,
        factors: list[str], neutral_methods: list[str], test_windows: list[int],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for factor, neutral_method, test_window in itertools.product(factors, neutral_methods, test_windows):
        pool.apply_async(ic_test_neutral, args=(factor, neutral_method, test_window), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... ICN calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
