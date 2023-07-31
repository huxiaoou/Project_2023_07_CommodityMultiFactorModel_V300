import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriterByDate
from skyrim.whiterun import CCalendar
from factors.factors_shared import neutralize_by_sector


def cal_test_return_neutral(
        test_window: int, neutral_method: str,
        run_mode: str, bgn_date: str, stp_date: str | None,
        instruments_universe: list[str],
        available_universe_dir,
        sector_classification: dict[str, str],
        test_return_dir: str,
        test_return_neutral_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1]):
    # --- calendar
    cne_calendar = CCalendar(t_path=calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- available universe
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.set_default(available_universe_lib_structure.m_tab.m_table_name)

    # --- mother universe
    mother_universe_df = pd.DataFrame({"instrument": instruments_universe})

    # --- sector df
    sector_df = pd.DataFrame.from_dict({z: {sector_classification[z]: 1} for z in instruments_universe}, orient="index").fillna(0)

    weight_id = "WGT{:02d}".format(test_window)

    # --- test return library
    test_return_lib_id = "test_return_{:03d}".format(test_window)
    test_return_lib_structure = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=test_return_dir)
    test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)

    # --- test return neutral library
    test_return_neutral_lib_id = "test_return_{:03d}.{}".format(test_window, neutral_method)
    test_return_neutral_lib_structure = database_structure[test_return_neutral_lib_id]
    test_return_neutral_lib = CManagerLibWriterByDate(t_db_name=test_return_neutral_lib_structure.m_lib_name, t_db_save_dir=test_return_neutral_dir)
    test_return_neutral_lib.initialize_table(t_table=test_return_neutral_lib_structure.m_tab, t_remove_existence=run_mode in ["O"])

    # --- update by date
    for trade_date in cne_calendar.get_iter_list(t_bgn_date=bgn_date, t_stp_date=stp_date, t_ascending=True):
        test_return_df = test_return_lib.read_by_date(
            t_trade_date=trade_date,
            t_value_columns=["instrument", "value"]
        )
        if len(test_return_df) == 0:
            print(f"... Warning! test_window = {test_window:>2d} trade_date = {trade_date} Not enough test return")
            continue
        weight_df = available_universe_lib.read_by_date(
            t_trade_date=trade_date,
            t_value_columns=["instrument", weight_id]
        )
        if len(weight_df) == 0:
            print(f"... Warning! test_window = {test_window:>2d} trade_date = {trade_date} Not enough weight data")
            continue

        input_df = mother_universe_df.merge(
            right=weight_df, on=["instrument"], how="inner").merge(
            right=test_return_df, on=["instrument"], how="inner").set_index("instrument")

        if neutral_method == "WE":
            input_df[weight_id] = 1
        elif neutral_method == "WS":
            input_df[weight_id] = np.sqrt(input_df[weight_id])
        test_return_neutral_srs = neutralize_by_sector(
            t_raw_data=input_df["value"],
            t_sector_df=sector_df,
            t_weight=input_df[weight_id]
        )
        test_return_neutral_lib.update_by_date(
            t_date=trade_date,
            t_update_df=pd.DataFrame({"value": test_return_neutral_srs}),
            t_using_index=True
        )

    test_return_lib.close()
    test_return_neutral_lib.close()
    available_universe_lib.close()
    return 0


def cal_test_return_neutral_mp(proc_num: int,
                               test_windows: list[int],
                               neutral_method: str,
                               run_mode: str, bgn_date: str, stp_date: str | None,
                               instruments_universe: list[str],
                               available_universe_dir,
                               sector_classification: dict[str, str],
                               test_return_dir: str,
                               test_return_neutral_dir: str,
                               calendar_path: str,
                               database_structure: dict[str, CLib1Tab1]
                               ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in test_windows:
        pool.apply_async(cal_test_return_neutral,
                         args=(p_window,
                               neutral_method,
                               run_mode, bgn_date, stp_date,
                               instruments_universe,
                               available_universe_dir,
                               sector_classification,
                               test_return_dir,
                               test_return_neutral_dir,
                               calendar_path,
                               database_structure))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... test return neutral calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
