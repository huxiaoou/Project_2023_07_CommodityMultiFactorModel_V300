import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CManagerLibWriter, CLib1Tab1
from skyrim.whiterun import CCalendar


def find_price(t_x: pd.Series, t_md_df: pd.DataFrame):
    _trade_date = t_x.name
    try:
        n_prc = t_md_df.at[_trade_date, t_x["n_contract"]]
    except KeyError:
        n_prc = np.nan
    try:
        d_prc = t_md_df.at[_trade_date, t_x["d_contract"]]
    except KeyError:
        d_prc = np.nan
    return n_prc, d_prc


def cal_roll_return(t_x: pd.Series, t_n_prc_lbl: str, t_d_prc_lbl: str, t_ret_scale: int):
    d_month, n_month = t_x["d_contract"].split(".")[0][-2:], t_x["n_contract"].split(".")[0][-2:]
    _dlt_month = int(d_month) - int(n_month)
    _dlt_month = _dlt_month + (12 if _dlt_month <= 0 else 0)
    return (t_x[t_n_prc_lbl] / t_x[t_d_prc_lbl] - 1) / _dlt_month * 12 * t_ret_scale


def factors_algorithm_TS(
        ts_window: int,
        run_mode: str, bgn_date: str, stp_date: str,
        concerned_instruments_universe: list[str],
        factors_exposure_dir: str,
        major_minor_dir: str,
        md_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        price_type: str = "close",
        return_scale: int = 100,
):
    factor_lbl = "TS{:03d}".format(ts_window)

    # set dates
    calendar = CCalendar(calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -ts_window + 1)

    # --- calculate factors by instrument
    all_factor_data = {}
    for instrument in concerned_instruments_universe:
        major_minor_file = "major_minor.{}.csv.gz".format(instrument)
        major_minor_path = os.path.join(major_minor_dir, major_minor_file)
        major_minor_df = pd.read_csv(major_minor_path, dtype={"trade_date": str}).set_index("trade_date")
        filter_dates = (major_minor_df.index >= base_date) & (major_minor_df.index < stp_date)
        major_minor_df = major_minor_df.loc[filter_dates]

        md_file = "{}.md.{}.csv.gz".format(instrument, price_type)
        md_path = os.path.join(md_dir, md_file)
        md_df = pd.read_csv(md_path, dtype={"trade_date": str}).set_index("trade_date")

        major_minor_df["n_" + price_type], major_minor_df["d_" + price_type] = zip(*major_minor_df.apply(find_price, args=(md_df,), axis=1))
        major_minor_df["roll_return"] = major_minor_df.apply(cal_roll_return, args=("n_" + price_type, "d_" + price_type, return_scale), axis=1)
        major_minor_df["roll_return"] = major_minor_df["roll_return"].fillna(method="ffill").rolling(window=ts_window).mean()
        major_minor_df = major_minor_df.loc[major_minor_df.index >= bgn_date]
        all_factor_data[instrument] = major_minor_df["roll_return"]

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


def cal_factors_exposure_ts_mp(
        proc_num: int,
        ts_windows: list[int],
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p in ts_windows:
        pool.apply_async(factors_algorithm_TS, args=(p,), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... factor TS calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
