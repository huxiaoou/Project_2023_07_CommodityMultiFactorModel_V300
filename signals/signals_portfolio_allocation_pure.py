import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriterByDate


def signals_pure(
        pure_portfolio_id: str, pure_portfolio_options: dict[str, dict],
        run_mode: str, bgn_date: str, stp_date: str | None,
        signals_dir: str,
        signals_allocation_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
):
    pure_portfolio_details_subsets = pure_portfolio_options[pure_portfolio_id]

    # --- set dates
    cne_calendar = CCalendar(t_path=calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- initialize
    allocation_lib = CManagerLibWriterByDate(t_db_name=database_structure[pure_portfolio_id].m_lib_name, t_db_save_dir=signals_allocation_dir)
    allocation_lib.initialize_table(t_table=database_structure[pure_portfolio_id].m_tab, t_remove_existence=run_mode in ["O"])

    # --- load component factors
    signal_readers = {}
    for pure_portfolio_details in pure_portfolio_details_subsets.values():
        for comp_factor, comp_factor_id in pure_portfolio_details.items():
            if comp_factor not in signal_readers:
                signal_readers[comp_factor] = CManagerLibReader(t_db_name=database_structure[comp_factor_id].m_lib_name, t_db_save_dir=signals_dir)
                signal_readers[comp_factor].set_default(database_structure[comp_factor_id].m_tab.m_table_name)

    # --- init before loop
    key_dates = list(pure_portfolio_details_subsets)
    key_dates.sort(reverse=True)
    win_i = 0
    while key_dates[win_i] > bgn_date:
        win_i += 1
    pure_portfolio_details = pure_portfolio_details_subsets[key_dates[win_i]]

    # --- core daily loop
    for trade_date in cne_calendar.get_iter_list(t_bgn_date=bgn_date, t_stp_date=stp_date, t_ascending=True):
        if trade_date >= key_dates[win_i - 1]:
            win_i -= 1
            pure_portfolio_details = pure_portfolio_details_subsets[key_dates[win_i]]
        comp_weight_data = {}
        for comp_factor, comp_factor_id in pure_portfolio_details.items():
            comp_factor_df = signal_readers[comp_factor].read_by_date(
                t_trade_date=trade_date, t_value_columns=["instrument", "value"]
            ).set_index("instrument")
            comp_weight_data[comp_factor] = comp_factor_df["value"]
        comp_weight_df = pd.DataFrame(comp_weight_data)
        allocation_aver_weight_srs = comp_weight_df.mean(axis=1)
        allocation_weight_srs = allocation_aver_weight_srs / allocation_aver_weight_srs.abs().sum()
        allocation_weight_df = pd.DataFrame({"value": allocation_weight_srs})
        allocation_lib.update_by_date(
            t_date=trade_date,
            t_update_df=allocation_weight_df,
            t_using_index=True
        )

    # --- close component factors library
    for comp_factor, comp_factor_lib in signal_readers.items():
        comp_factor_lib.close()
    allocation_lib.close()
    return 0


def cal_signals_pure_mp(
        proc_num: int,
        pure_portfolio_ids: list[str], pure_portfolio_options: dict[str, dict[str, dict]],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for ppid in pure_portfolio_ids:
        pool.apply_async(signals_pure, args=(ppid, pure_portfolio_options), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... portfolio pure calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
