import sys
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriterByDate


def get_weight_from_factor_head_tail_equal(t_raw_factor_srs: pd.Series, t_single_hold_prop: float):
    tot_size = len(t_raw_factor_srs)
    k0 = max(min(int(np.ceil(tot_size * t_single_hold_prop)), int(tot_size / 2)), 1)
    k1 = tot_size - 2 * k0
    t_weight_srs = pd.Series(
        data=[1 / 2 / k0] * k0 + [0.0] * k1 + [-1 / 2 / k0] * k0,
        index=t_raw_factor_srs.sort_values(ascending=False).index
    )
    return t_weight_srs


def signals_raw(
        raw_portfolio_id: str, raw_portfolio_options: dict[str, dict[str, dict]],
        run_mode: str, bgn_date: str, stp_date: str | None,
        test_universe: list[str],
        available_universe_dir: str,
        factors_exposure_dir: str,
        signals_allocation_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
):
    # --- load components of synth factor
    raw_portfolio_details = raw_portfolio_options[raw_portfolio_id]
    weight_srs = pd.Series({k: v["weight"] for k, v in raw_portfolio_details.items()})

    # --- set dates
    cne_calendar = CCalendar(t_path=calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- set test universe
    test_universe_srs = pd.Series(data=1, index=test_universe)

    # --- initialize
    allocation_lib = CManagerLibWriterByDate(t_db_name=database_structure[raw_portfolio_id].m_lib_name, t_db_save_dir=signals_allocation_dir)
    allocation_lib.initialize_table(t_table=database_structure[raw_portfolio_id].m_tab, t_remove_existence=run_mode in ["O"])

    # --- load available universe
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.set_default(available_universe_lib_structure.m_tab.m_table_name)

    # --- load component factors
    signal_readers: dict[str, CManagerLibReader] = {}
    for comp_factor in raw_portfolio_details:
        signal_readers[comp_factor] = CManagerLibReader(t_db_name=database_structure[comp_factor].m_lib_name, t_db_save_dir=factors_exposure_dir)
        signal_readers[comp_factor].set_default(database_structure[comp_factor].m_tab.m_table_name)

    # --- main loop
    for trade_date in cne_calendar.get_iter_list(bgn_date, stp_date, True):
        # load available universe
        available_universe_df = available_universe_lib.read_by_date(
            t_trade_date=trade_date,
            t_value_columns=["instrument", "amount"]
        ).set_index("instrument")
        if len(available_universe_df) == 0:
            print(f"... Error! No available universe for {raw_portfolio_id} @ {trade_date}.")
            print("... This program will terminate at once. Please check again.")
            sys.exit()

        # get intersection with test universe
        available_universe_df["target"] = test_universe_srs
        available_universe_df = available_universe_df.loc[available_universe_df["target"] == 1]

        # load weight for each factor
        weight_data = {}
        for comp_factor, comp_factor_config in raw_portfolio_details.items():
            factor_df = signal_readers[comp_factor].read_by_date(
                t_trade_date=trade_date,
                t_table_name=database_structure[comp_factor].m_tab.m_table_name,
                t_value_columns=["instrument", "value"]
            ).set_index("instrument")
            available_universe_df[comp_factor] = factor_df["value"]  # use this to drop instruments that are not available
            weight_data[comp_factor] = get_weight_from_factor_head_tail_equal(
                t_raw_factor_srs=available_universe_df[comp_factor],
                t_single_hold_prop=comp_factor_config["SHP"]
            )
        weight_df = pd.DataFrame(weight_data)
        weight_df_aver_srs = weight_df.dot(weight_srs)
        weight_df[raw_portfolio_id] = weight_df_aver_srs / weight_df_aver_srs.abs().sum()
        allocation_lib.update_by_date(
            t_date=trade_date,
            t_update_df=weight_df[[raw_portfolio_id]],
            t_using_index=True
        )

    # --- close component factors library
    available_universe_lib.close()
    for comp_factor, comp_factor_lib in signal_readers.items():
        comp_factor_lib.close()
    allocation_lib.close()
    return 0


def cal_signals_raw_mp(
        proc_num: int,
        raw_portfolio_ids: list[str], raw_portfolio_options: dict[str, dict[str, dict]],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for rpid in raw_portfolio_ids:
        pool.apply_async(signals_raw, args=(rpid, raw_portfolio_options), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... portfolio raw calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
