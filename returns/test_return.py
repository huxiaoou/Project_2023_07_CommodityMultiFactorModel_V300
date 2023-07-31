import os
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibWriter
from skyrim.whiterun import CCalendar


def cal_test_return(test_window: int,
                    run_mode: str, bgn_date: str, stp_date: str | None,
                    instruments_return_dir: str,
                    test_return_dir: str,
                    calendar_path: str,
                    database_structure: dict[str, CLib1Tab1],
                    ret_scale: int = 100):
    # set dates
    calendar = CCalendar(calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -test_window)

    # load raw return
    raw_return_file = "instruments.return.csv.gz"
    raw_return_path = os.path.join(instruments_return_dir, raw_return_file)
    raw_return_df = pd.read_csv(raw_return_path, dtype={"trade_date": str}).set_index("trade_date")
    filter_dates = (raw_return_df.index >= base_date) & (raw_return_df.index < stp_date)
    raw_return_df = raw_return_df.loc[filter_dates]

    nav_df = (raw_return_df / ret_scale + 1).cumprod()
    rolling_return_df = (nav_df / nav_df.shift(test_window) - 1) * ret_scale
    test_rolling_return_df = rolling_return_df[rolling_return_df.index >= bgn_date]
    update_df = test_rolling_return_df.stack().reset_index(level=1)

    # --- initialize lib
    test_return_lib_id = "test_return_{:03d}".format(test_window)
    test_return_lib_structure = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibWriter(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=test_return_dir)
    test_return_lib.initialize_table(t_table=test_return_lib_structure.m_tab, t_remove_existence=run_mode in ["O"])
    test_return_lib.update(t_update_df=update_df, t_using_index=True)
    test_return_lib.close()
    return 0


def cal_test_return_mp(proc_num: int,
                       test_windows: list[int],
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       instruments_return_dir: str,
                       test_return_dir: str,
                       calendar_path: str,
                       database_structure: dict[str, CLib1Tab1],
                       ret_scale: int = 100
                       ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in test_windows:
        pool.apply_async(cal_test_return,
                         args=(p_window,
                               run_mode, bgn_date, stp_date,
                               instruments_return_dir,
                               test_return_dir,
                               calendar_path,
                               database_structure,
                               ret_scale))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... test return calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
