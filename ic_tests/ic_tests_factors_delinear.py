import itertools
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriter
from skyrim.whiterun import CCalendar


def corr_one_day_delinear(df: pd.DataFrame, xs: list[str], y: str, method: str):
    res = df[xs].corrwith(df[y], axis=0, method=method)
    return res


def ic_test_delinear(pid: str, selected_factors_pool: list[str],
                     neutral_method: str, test_window: int, factors_return_lag: int,
                     run_mode: str, bgn_date: str, stp_date: str,
                     ic_tests_delinear_dir: str,
                     exposure_dir: str,
                     return_dir: str,
                     calendar_path: str,
                     database_structure: dict[str, CLib1Tab1],
                     ):
    __test_lag = 1

    # --- set dates
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_bgn_date = calendar.get_next_date(iter_dates[0], -__test_lag - test_window)
    base_stp_date = calendar.get_next_date(iter_dates[-1], -__test_lag - test_window + 1)
    base_dates = calendar.get_iter_list(base_bgn_date, base_stp_date, True)
    bridge_dates_df = pd.DataFrame({"base_date": base_dates, "trade_date": iter_dates})

    # --- load delinearized factors
    delinear_lib_id = "{}.{}.DELINEAR".format(pid, neutral_method)
    delinear_lib = CManagerLibReader(t_db_save_dir=exposure_dir, t_db_name=database_structure[delinear_lib_id].m_lib_name)
    delinear_lib.set_default(database_structure[delinear_lib_id].m_tab.m_table_name)
    delinearized_df = delinear_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument"] + selected_factors_pool)
    delinear_lib.close()

    # --- load test return
    test_return_lib_id = "test_return_{:03d}.{}".format(test_window, neutral_method)
    test_return_lib_structure = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=return_dir)
    test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)
    test_return_df = test_return_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])
    test_return_lib.close()

    # --- merge exposure and return
    factors_exposure_df = delinearized_df.rename(mapper={"trade_date": "base_date"}, axis=1)
    test_return_df_expand = pd.merge(
        left=bridge_dates_df, right=test_return_df,
        on="trade_date", how="right",
    )
    test_input_df = pd.merge(
        left=factors_exposure_df, right=test_return_df_expand,
        on=["base_date", "instrument"], how="inner"
    )
    res_df = test_input_df.groupby(by="trade_date", group_keys=True).apply(corr_one_day_delinear, xs=selected_factors_pool, y="value", method="spearman")
    update_df = res_df.stack().reset_index(level=1)

    # --- initialize output lib
    ic_test_lib_id = f"ic-delinear-{pid}-{neutral_method}-TW{test_window:03d}-T{factors_return_lag}"
    ic_test_lib = CManagerLibWriter(t_db_save_dir=ic_tests_delinear_dir, t_db_name=database_structure[ic_test_lib_id].m_lib_name)
    ic_test_lib.initialize_table(t_table=database_structure[ic_test_lib_id].m_tab, t_remove_existence=run_mode in ["O"])
    ic_test_lib.update(t_update_df=update_df, t_using_index=True)
    ic_test_lib.close()
    return 0


def cal_ic_tests_delinear_mp(
        proc_num: int,
        pids: list[str], factors_pool_options: dict[str, list[str]],
        neutral_methods: list[str], test_windows: list[int], factors_return_lags: list[int],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for pid, neutral_method, test_window, factors_return_lag in itertools.product(
            pids, neutral_methods, test_windows, factors_return_lags):
        selected_factors_pool = factors_pool_options[pid]
        pool.apply_async(ic_test_delinear, args=(
            pid, selected_factors_pool,
            neutral_method, test_window, factors_return_lag), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... ICD calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
