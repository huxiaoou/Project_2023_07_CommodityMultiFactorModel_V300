import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CManagerLibWriter, CLib1Tab1
from skyrim.whiterun import CCalendar


def cal_wgt_ary(t_half_life: int, t_size: int, t_ascending: bool):
    """

    :param t_half_life:
    :param t_size:
    :param t_ascending: if true, the near days would have more weight, otherwise less weight.
    :return:
    """
    _rou = np.power(0.5, 1 / t_half_life)
    _ticks = np.arange(t_size, 0, -1) if t_ascending else np.arange(0, t_size, 1)
    _w = np.power(_rou, _ticks)
    _w = _w / _w.sum()
    return _w


def cal_registered_stock_change_ratio(t_x: pd.Series, t_this_lbl: str, t_prev_lbl: str, t_lower_lim: float, t_ret_scale: int):
    if t_x[t_prev_lbl] >= t_lower_lim:
        return -(t_x[t_this_lbl] / t_x[t_prev_lbl] - 1) * t_ret_scale
    else:
        return np.nan


def factors_algorithm_RSW(
        rs_window: int, half_life: int,
        run_mode: str, bgn_date: str, stp_date: str,
        concerned_instruments_universe: list[str],
        factors_exposure_dir: str,
        fundamental_dir: str,
        major_minor_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        return_scale: int = 100,
):
    factor_lbl = "RSW{:03d}HL{:03d}".format(rs_window, half_life)
    lag_label = "in_stock_L{}_HL{}".format(rs_window, half_life)
    rsw_lower_lim = 10
    wgt_ary = cal_wgt_ary(t_half_life=half_life, t_size=rs_window, t_ascending=False)

    # set dates
    calendar = CCalendar(calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -rs_window + 1)

    # --- calculate factors by instrument
    all_factor_data = {}
    for instrument in concerned_instruments_universe:
        instrument_file = "{}.STOCK.csv.gz".format(instrument)
        instrument_path = os.path.join(fundamental_dir, instrument_file)
        instrument_df = pd.read_csv(instrument_path, dtype={"trade_date": str}).set_index("trade_date")

        major_minor_file = "major_minor.{}.csv.gz".format(instrument)
        major_minor_path = os.path.join(major_minor_dir, major_minor_file)
        major_minor_df = pd.read_csv(major_minor_path, dtype={"trade_date": str}).set_index("trade_date")
        filter_dates = (major_minor_df.index >= base_date) & (major_minor_df.index < stp_date)
        major_minor_df = major_minor_df.loc[filter_dates]

        instrument_df: pd.DataFrame = pd.merge(left=major_minor_df, right=instrument_df, how="left", left_index=True, right_index=True)
        instrument_df = instrument_df.fillna(method="ffill").fillna(0)
        instrument_df[lag_label] = instrument_df["in_stock"].rolling(window=rs_window).apply(lambda z: z @ wgt_ary)
        instrument_df[factor_lbl] = instrument_df[["in_stock", lag_label]].apply(
            cal_registered_stock_change_ratio, args=("in_stock", lag_label, rsw_lower_lim, return_scale), axis=1)
        instrument_df = instrument_df.loc[instrument_df.index >= bgn_date]
        all_factor_data[instrument] = instrument_df[factor_lbl]

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


def cal_factors_exposure_rsw_mp(
        proc_num: int,
        rsw_windows: list[int], half_life_windows: list[int],
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p in rsw_windows:
        for q in half_life_windows:
            pool.apply_async(factors_algorithm_RSW, args=(p, q), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... factor RSW calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
