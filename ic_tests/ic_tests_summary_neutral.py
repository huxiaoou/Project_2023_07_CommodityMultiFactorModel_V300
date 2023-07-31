import itertools
import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.winterhold import plot_lines


def ic_tests_summary_neutral(
        factor: str, neutral_method: str,
        test_windows: list[int],
        ic_tests_dir: str,
        database_structure: dict[str, CLib1Tab1],
        days_per_year: int = 252):
    ic_cumsum_data = {}
    statistics_data = []
    for test_window in test_windows:
        test_id = f"{factor}-{neutral_method}-TW{test_window:03d}"
        ic_test_lib_id = f"ic-{test_id}"
        ic_test_lib_structure = database_structure[ic_test_lib_id]
        ic_test_lib = CManagerLibReader(t_db_name=ic_test_lib_structure.m_lib_name, t_db_save_dir=os.path.join(ic_tests_dir, factor))
        ic_test_lib.set_default(ic_test_lib_structure.m_tab.m_table_name)
        ic_df = ic_test_lib.read(t_value_columns=["trade_date", "value"]).set_index("trade_date")
        ic_df.rename(mapper={"value": "ic"}, axis=1, inplace=True)
        ic_df["cum_ic"] = ic_df["ic"].cumsum()
        ic_test_lib.close()
        ic_cumsum_data[test_id] = ic_df["cum_ic"]

        # get statistic summary
        obs = len(ic_df)
        mu = ic_df["ic"].mean()
        sd = ic_df["ic"].std()
        icir = mu / sd * np.sqrt(days_per_year / test_window)
        ic_q000 = np.percentile(ic_df["ic"], q=0)
        ic_q025 = np.percentile(ic_df["ic"], q=25)
        ic_q050 = np.percentile(ic_df["ic"], q=50)
        ic_q075 = np.percentile(ic_df["ic"], q=75)
        ic_q100 = np.percentile(ic_df["ic"], q=100)
        statistics_data.append({
            "factor": factor,
            "testWindow": test_window,
            "obs": obs,
            "IC-Mean": mu,
            "IC-Std": sd,
            "ICIR": icir,
            "q000": ic_q000,
            "q025": ic_q025,
            "q050": ic_q050,
            "q075": ic_q075,
            "q100": ic_q100,
        })

    ic_cumsum_df = pd.DataFrame(ic_cumsum_data)
    plot_lines(
        t_plot_df=ic_cumsum_df,
        t_fig_name="ic_cumsum.{}.{}".format(factor, neutral_method),
        t_save_dir=os.path.join(ic_tests_dir, factor),
        t_colormap="jet"
    )

    statistics_df = pd.DataFrame(statistics_data)
    statistics_file = "statistics.{}.{}.csv".format(factor, neutral_method)
    statistics_path = os.path.join(ic_tests_dir, factor, statistics_file)
    statistics_df.to_csv(statistics_path, index=False, float_format="%.4f")
    return 0


def cal_ic_tests_neutral_summary_mp(
        proc_num: int,
        factors: list[str], neutral_methods: list[str],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for factor, neutral_method in itertools.product(factors, neutral_methods):
        pool.apply_async(ic_tests_summary_neutral, args=(factor, neutral_method), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... ICNS calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
