import datetime as dt
import itertools
import multiprocessing as mp
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriterByDate


def pure_factors_vanilla(
        test_window: int, pid: str, neutral_method: str, factors_return_lag: int,
        run_mode: str, bgn_date: str, stp_date: str | None,
        src_factors: list[str],
        factors_portfolio_dir: str,
        signals_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
):
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    test_id = "{}.{}.TW{:03d}.T{}".format(pid, neutral_method, test_window, factors_return_lag)

    # --- load calendar
    cne_calendar = CCalendar(t_path=calendar_path)

    # --- pure factor portfolio data
    factors_portfolio_lib_id = "factors_portfolio.{}".format(test_id)
    factors_portfolio_lib = CManagerLibReader(t_db_save_dir=factors_portfolio_dir, t_db_name=database_structure[factors_portfolio_lib_id].m_lib_name)
    factors_portfolio_lib.set_default(database_structure[factors_portfolio_lib_id].m_tab.m_table_name)

    # --- signals writer
    signals_writers = {}
    for factor in src_factors:
        signal_lib_id = "pure_factors_VANILLA.{}.TW{:03d}".format(factor, test_window)
        signal_lib = CManagerLibWriterByDate(t_db_save_dir=signals_dir, t_db_name=database_structure[signal_lib_id].m_lib_name)
        signal_lib.initialize_table(t_table=database_structure[signal_lib_id].m_tab, t_remove_existence=run_mode in ["O"])
        signals_writers[factor] = signal_lib

    # --- main loop
    for trade_date in cne_calendar.get_iter_list(bgn_date, stp_date, True):
        factors_portfolio_df = factors_portfolio_lib.read_by_date(
            t_trade_date=trade_date,
            t_value_columns=["instrument"] + src_factors
        ).set_index("instrument")
        wgt_abs_sum = factors_portfolio_df.abs().sum()
        wgt_norm_df = factors_portfolio_df / wgt_abs_sum
        wgt_df = wgt_norm_df
        for factor in src_factors:
            signal_lib = signals_writers[factor]
            signal_lib.update_by_date(
                t_date=trade_date,
                t_update_df=wgt_df[[factor]],
                t_using_index=True
            )
    for factor in src_factors:
        signals_writers[factor].close()
    factors_portfolio_lib.close()
    return 0


def cal_signals_vanilla_mp(
        proc_num: int,
        test_windows: list[int], pids: list[str], neutral_methods: list[str], factors_return_lags: list[int],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for tw, p, nm, lag in itertools.product(test_windows, pids, neutral_methods, factors_return_lags):
        pool.apply_async(pure_factors_vanilla, args=(tw, p, nm, lag), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... signals - pure factors - VANILLA ALL calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
