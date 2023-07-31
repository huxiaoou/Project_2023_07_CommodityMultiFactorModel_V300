import os
import datetime as dt
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1, CManagerLibWriter


def get_available_universe_by_date(x: pd.Series, ret_df: pd.DataFrame, amt_df: pd.DataFrame, dfs: dict[int, pd.DataFrame], res: []):
    trade_date = x.name
    available_universe_df = pd.DataFrame({
        "return": ret_df.loc[trade_date, x],
        "amount": amt_df.loc[trade_date, x],
    })
    for tw, df in dfs.items():
        available_universe_df["WGT{:02d}".format(tw)] = df.loc[trade_date, x]
    available_universe_df["trade_date"] = trade_date
    res.append(available_universe_df)
    return 0


def cal_available_universe(
        test_windows: list[int],
        run_mode: str, bgn_date: str, stp_date: str | None,
        instruments_universe: list[str],
        available_universe_options: dict[str, int | float],
        available_universe_dir: str,
        major_return_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        price_type: str = "close"):
    t0 = dt.datetime.now()
    _wanyuan, _yiyuan = 1e4, 1e8
    rolling_win, amt_threshold = available_universe_options["rolling_window"], available_universe_options["amount_threshold"]

    # set dates
    calendar = CCalendar(calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -max(test_windows + [rolling_win]) + 1)

    # --- initialize lib
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibWriter(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.initialize_table(t_table=available_universe_lib_structure.m_tab, t_remove_existence=run_mode in ["O"])

    # --- load all amount and return data
    amt_ma_data_for_available, amt_ma_data_for_test = {}, {test_window: {} for test_window in test_windows}
    amt_data, return_data = {}, {}
    for instrument in instruments_universe:
        major_return_file = "major_return.{}.{}.csv.gz".format(instrument, price_type)
        major_return_path = os.path.join(major_return_dir, major_return_file)
        major_return_df = pd.read_csv(major_return_path, dtype={"trade_date": str}).set_index("trade_date")
        filter_dates = (major_return_df.index >= base_date) & (major_return_df.index < stp_date)
        major_return_df = major_return_df.loc[filter_dates]
        amt_ma_data_for_available[instrument] = major_return_df["amount"].rolling(window=rolling_win).mean() * _wanyuan / _yiyuan
        for test_window in test_windows:
            amt_ma_data_for_test[test_window][instrument] = major_return_df["amount"].rolling(window=test_window).mean() * _wanyuan / _yiyuan
        amt_data[instrument] = major_return_df["amount"] * _wanyuan / _yiyuan
        return_data[instrument] = major_return_df["major_return"]

    # --- reorganize and save
    amt_ma_df_for_available = pd.DataFrame(amt_ma_data_for_available)
    amt_ma_df_for_test = {tw: pd.DataFrame(d) for tw, d in amt_ma_data_for_test.items()}
    amt_df = pd.DataFrame(amt_data)
    return_df = pd.DataFrame(return_data)
    filter_df = amt_ma_df_for_available >= amt_threshold
    filter_df = filter_df.loc[filter_df.index >= bgn_date]
    res = []
    filter_df.apply(get_available_universe_by_date, args=(return_df, amt_df, amt_ma_df_for_test, res), axis=1)
    update_df = pd.concat(res).reset_index().set_index("trade_date").sort_index(ascending=True)
    available_universe_lib.update(t_update_df=update_df, t_using_index=True)
    available_universe_lib.close()

    print("\n... available universe are calculated")
    print("... total time consuming: {:.2f} seconds".format((dt.datetime.now() - t0).total_seconds()))

    return 0
