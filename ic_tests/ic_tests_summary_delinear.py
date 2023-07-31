import os
import itertools
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.winterhold import plot_lines


def ic_tests_delinear_summary(pid: str, selected_factors_pool: list[str],
                              neutral_method: str, test_window: int, factors_return_lag: int,
                              ic_tests_delinear_dir: str,
                              database_structure: dict[str, CLib1Tab1],
                              days_per_year: int = 252,
                              ):
    ic_test_lib_id = f"ic-delinear-{pid}-{neutral_method}-TW{test_window:03d}-T{factors_return_lag}"
    ic_test_lib = CManagerLibReader(t_db_save_dir=ic_tests_delinear_dir, t_db_name=database_structure[ic_test_lib_id].m_lib_name)
    ic_test_lib.set_default(database_structure[ic_test_lib_id].m_tab.m_table_name)
    all_ic_df = ic_test_lib.read(
        t_table_name=database_structure[ic_test_lib_id].m_tab.m_table_name,
        t_value_columns=["trade_date", "factor", "value"]
    )
    ic_test_lib.close()

    # plot cumulative IC
    pivot_ic_df = pd.pivot_table(data=all_ic_df, index="trade_date", columns="factor", values="value", aggfunc=sum)
    pivot_ic_cumsum_df = pivot_ic_df[selected_factors_pool].cumsum()
    plot_lines(
        t_plot_df=pivot_ic_cumsum_df,
        t_save_dir=ic_tests_delinear_dir,
        t_fig_name=ic_test_lib_id,
        t_colormap="jet"
    )

    # get IC statistics
    mu = pivot_ic_df.mean()
    sd = pivot_ic_df.std()
    icir = mu / sd * np.sqrt(days_per_year / test_window)
    delinear_factor_ic_test_summary_df = pd.DataFrame.from_dict({
        "IC-Mean": mu,
        "IC-Std": sd,
        "ICIR": icir,
    }, orient="index").sort_index(axis=1)

    print(delinear_factor_ic_test_summary_df)
    delinear_factor_ic_test_summary_file = ic_test_lib_id + ".summary.csv"
    delinear_factor_ic_test_summary_path = os.path.join(ic_tests_delinear_dir, delinear_factor_ic_test_summary_file)
    delinear_factor_ic_test_summary_df.to_csv(delinear_factor_ic_test_summary_path, index_label="factor", float_format="%.3f")
    return 0


def cal_ic_tests_delinear_summary_mp(
        proc_num: int,
        pids: list[str], factors_pool_options: dict[str, list[str]],
        neutral_methods: list[str], test_windows: list[int], factors_return_lags: list[int],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for pid, neutral_method, test_window, factors_return_lag in itertools.product(
            pids, neutral_methods, test_windows, factors_return_lags):
        selected_factors_pool = factors_pool_options[pid]
        pool.apply_async(ic_tests_delinear_summary, args=(
            pid, selected_factors_pool,
            neutral_method, test_window, factors_return_lag), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... ICDS calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
