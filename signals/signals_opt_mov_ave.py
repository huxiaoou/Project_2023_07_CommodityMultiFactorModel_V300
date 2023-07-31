import datetime as dt
import itertools
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriterByDate


def signal_mov_ave(src_id: str, dst_id: str, mov_ave_len: int,
                   run_mode: str, bgn_date: str, stp_date: str | None,
                   minimum_abs_weight: float,
                   src_dir: str,
                   signals_opt_dir: str,
                   calendar_path: str,
                   database_structure: dict[str, CLib1Tab1],
                   ):
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    cne_calendar = CCalendar(t_path=calendar_path)
    iter_dates = cne_calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = cne_calendar.get_next_date(iter_dates[0], -mov_ave_len + 1)

    # --- init src lib
    src_lib = CManagerLibReader(t_db_save_dir=src_dir, t_db_name=database_structure[src_id].m_lib_name)
    src_lib.set_default(database_structure[src_id].m_tab.m_table_name)

    # --- init allocation opt lib
    dst_opt_lib = CManagerLibWriterByDate(
        t_db_save_dir=signals_opt_dir, t_db_name=database_structure[dst_id].m_lib_name)
    dst_opt_lib.initialize_table(database_structure[dst_id].m_tab, t_remove_existence=run_mode in ["O"])

    # --- main
    signal_queue = []
    for trade_date in cne_calendar.get_iter_list(base_date, stp_date, True):
        signal_df = src_lib.read_by_date(t_trade_date=trade_date, t_value_columns=["instrument", "value"]).set_index("instrument")
        if len(signal_df) > 0:
            signal_srs = signal_df["value"]
            signal_srs.name = trade_date

            # update queue, save the last mov_ave_len days' signal
            signal_queue.append(signal_srs)
            if len(signal_queue) > mov_ave_len:
                signal_queue.pop(0)

            # moving average of signals
            opt_df = pd.DataFrame(signal_queue).T
            opt_df["raw_ave"] = opt_df.fillna(0).mean(axis=1)
            filter_wgt_minimum = opt_df["raw_ave"].abs() > minimum_abs_weight
            opt_df = opt_df.loc[filter_wgt_minimum]
            opt_df["value"] = opt_df["raw_ave"] / opt_df["raw_ave"].abs().sum()

            # save
            if trade_date >= bgn_date:
                dst_opt_lib.update_by_date(
                    t_date=trade_date,
                    t_update_df=opt_df[["value"]],
                    t_using_index=True
                )

    src_lib.close()
    dst_opt_lib.close()
    return 0


def cal_signals_opt_raw_and_pure_mp(
        proc_num: int,
        portfolio_ids: list[str], mov_ave_lens: list[int],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for portfolio_id, mov_ave_len in itertools.product(portfolio_ids, mov_ave_lens):
        src_id = portfolio_id
        dst_id = "{}M{:03d}".format(src_id, mov_ave_len)
        pool.apply_async(signal_mov_ave, args=(src_id, dst_id, mov_ave_len), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... signals raw opt calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_signals_opt_vanilla_mp(
        proc_num: int,
        factors: list[str], mov_ave_lens: list[int],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for factor, mov_ave_len in itertools.product(factors, mov_ave_lens):
        src_id = "pure_factors_VANILLA.{}.TW{:03d}".format(factor, mov_ave_len)
        dst_id = "{}VM{:03d}".format(factor, mov_ave_len)
        pool.apply_async(signal_mov_ave, args=(src_id, dst_id, mov_ave_len), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... signals vanilla opt calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_signals_opt_ma_mp(
        proc_num: int,
        factors: list[str], mov_ave_lens: list[int], fast_n_slow_n_comb: list[tuple],
        **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for factor, mov_ave_len, (fn, sn) in itertools.product(factors, mov_ave_lens, fast_n_slow_n_comb):
        src_id = "pure_factors_MA.{}.TW{:03d}.FAST{:03d}.SLOW{:03d}".format(factor, mov_ave_len, fn, sn)
        dst_id = "{}F{:03d}S{:03d}M{:03d}".format(factor, fn, sn, mov_ave_len)
        pool.apply_async(signal_mov_ave, args=(src_id, dst_id, mov_ave_len), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("\n... signals ma opt calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
