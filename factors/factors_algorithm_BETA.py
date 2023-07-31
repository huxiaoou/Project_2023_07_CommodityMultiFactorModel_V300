import os
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.falkreath import CManagerLibWriter, CLib1Tab1
from skyrim.whiterun import CCalendar


def factors_algorithm_BETA(
        beta_window: int,
        run_mode: str, bgn_date: str, stp_date: str,
        concerned_instruments_universe: list[str],
        factors_exposure_dir: str,
        instruments_return_dir: str,
        major_return_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        price_type: str = "close",
):
    factor_lbl = "BETA{:03d}".format(beta_window)

    # set dates
    calendar = CCalendar(calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -beta_window + 1)

    # --- load market return
    market_index_file = "market.return.csv.gz"
    market_index_path = os.path.join(instruments_return_dir, market_index_file)
    market_index_df = pd.read_csv(market_index_path, dtype={"trade_date": str}).set_index("trade_date")

    # --- calculate factors by instrument
    all_factor_data = {}
    for instrument in concerned_instruments_universe:
        major_return_file = "major_return.{}.{}.csv.gz".format(instrument, price_type)
        major_return_path = os.path.join(major_return_dir, major_return_file)
        major_return_df = pd.read_csv(major_return_path, dtype={"trade_date": str}).set_index("trade_date")
        filter_dates = (major_return_df.index >= base_date) & (major_return_df.index < stp_date)
        major_return_df = major_return_df.loc[filter_dates]

        major_return_df["market"] = market_index_df["market"]
        major_return_df["xy"] = (major_return_df["major_return"] * major_return_df["market"]).rolling(window=beta_window).mean()
        major_return_df["xx"] = (major_return_df["market"] * major_return_df["market"]).rolling(window=beta_window).mean()
        major_return_df["y"] = major_return_df["major_return"].rolling(window=beta_window).mean()
        major_return_df["x"] = major_return_df["market"].rolling(window=beta_window).mean()
        major_return_df["cov_xy"] = major_return_df["xy"] - major_return_df["x"] * major_return_df["y"]
        major_return_df["cov_xx"] = major_return_df["xx"] - major_return_df["x"] * major_return_df["x"]
        major_return_df[factor_lbl] = major_return_df["cov_xy"] / major_return_df["cov_xx"]
        major_return_df = major_return_df.loc[major_return_df.index >= bgn_date]
        all_factor_data[instrument] = major_return_df[factor_lbl]

    # --- reorganize
    all_factor_df = pd.DataFrame(all_factor_data)
    update_df = all_factor_df.stack().reset_index(level=1)

    # --- save
    factor_lib_structure = database_structure[factor_lbl]
    factor_lib = CManagerLibWriter(
        t_db_name=factor_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O"])
    factor_lib.update(t_update_df=update_df, t_using_index=True)
    factor_lib.close()
    return 0


def cal_factors_exposure_beta_mp(
        proc_num: int,
        beta_windows: list[int],
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p in beta_windows:
        pool.apply_async(factors_algorithm_BETA, args=(p,), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... factor BETA calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
