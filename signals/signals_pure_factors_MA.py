import datetime as dt
import pandas as pd
import itertools
import multiprocessing as mp
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriterByDate
from factors.factors_shared import load_factor_return


def pure_factors_ma(
        test_window: int, pid: str, neutral_method: str, factors_return_lag: int,
        fast_n: int, slow_n: int,
        run_mode: str, bgn_date: str, stp_date: str | None,
        src_factors: list[str],
        factors_return_dir: str,
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

    # --- load lib: factors_return/instruments_residual
    factors_return_agg_df = load_factor_return(
        test_window=test_window, pid=pid, neutral_method=neutral_method, factors_return_lag=factors_return_lag,
        loading_factors=src_factors,
        factors_return_dir=factors_return_dir,
        database_structure=database_structure,
    )
    factors_return_agg_df = factors_return_agg_df[src_factors]
    factors_return_agg_cumsum_df = factors_return_agg_df.cumsum()
    ma_fast_df: pd.DataFrame = factors_return_agg_cumsum_df.ewm(halflife=fast_n, adjust=False).mean()
    ma_slow_df: pd.DataFrame = factors_return_agg_cumsum_df.ewm(halflife=slow_n, adjust=False).mean()
    cond_df: pd.DataFrame = ma_fast_df > ma_slow_df
    direction_df = cond_df.applymap(lambda z: 1 if z else -1)  # no shift and no fillna

    # --- pure factor portfolio data
    factors_portfolio_lib_id = "factors_portfolio.{}".format(test_id)
    factors_portfolio_lib = CManagerLibReader(t_db_save_dir=factors_portfolio_dir, t_db_name=database_structure[factors_portfolio_lib_id].m_lib_name)
    factors_portfolio_lib.set_default(database_structure[factors_portfolio_lib_id].m_tab.m_table_name)

    # --- signals writer
    signals_writers = {}
    for factor in src_factors:
        signal_lib_id = "pure_factors_MA.{}.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(factor, test_window, fast_n, slow_n)
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
        direction_srs = direction_df.loc[trade_date]
        wgt_df = wgt_norm_df.apply(lambda z: z * direction_srs, axis=1)
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


def cal_signals_ma_mp(
        proc_num: int,
        test_windows: list[int], pids: list[str], neutral_methods: list[str], factors_return_lags: list[int],
        fast_n_slow_n_comb: list[tuple],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for tw, p, nm, lag, (fn, sn) in itertools.product(test_windows, pids, neutral_methods, factors_return_lags, fast_n_slow_n_comb):
        pool.apply_async(pure_factors_ma, args=(tw, p, nm, lag, fn, sn), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... signals - pure factors - MA timing calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
