import itertools
import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.winterhold import plot_bar
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriterByDate


def cal_risk_factor_return_colinear(t_r: np.ndarray, t_x: np.ndarray, t_instru_wgt: np.ndarray, t_sector_wgt: np.ndarray):
    """

    :param t_r: n x 1, n: number of instruments
    :param t_x: n x K, K = 1 + k, k = p + q; p = number of sectors; q = number of style factors; 1 = market; K: number of all risk factors
    :param t_instru_wgt: n x 1, weight (market value) for each instrument
    :param t_sector_wgt: p x 1, weight (market value) of each sector
    :return:
    """
    _n, _K = t_x.shape
    _p = len(t_sector_wgt)
    _q = _K - 1 - _p
    _R11_up_ = np.diag(np.ones(_p))  # p x p
    _R11_dn_ = np.concatenate(
        ([0], -t_sector_wgt[:-1] / t_sector_wgt[-1]))  # 1 x p, linear constrain: \sum_{i=1}^kw_iR_i = 0, R_i: sector return, output of this function
    _R11 = np.vstack((_R11_up_, _R11_dn_))  # (p + 1) x p
    _R12 = np.zeros(shape=(_p + 1, _q))
    _R21 = np.zeros(shape=(_q, _p))
    _R22 = np.diag(np.ones(_q))
    _R = np.vstack((np.hstack((_R11, _R12)), np.hstack((_R21, _R22))))  # (p + q + 1) x (p + q) = K x (K - 1)
    _v_raw = np.sqrt(t_instru_wgt)
    _v = np.diag(_v_raw / np.sum(_v_raw))  # n x n

    #
    # Omega = R((XR)'VXR)^{-1} (XR)'V # size = K x n
    # f = Omega * r
    # Omega * X = E_{kk} # size = K x K
    # Omega *XR = R

    _XR = t_x.dot(_R)  # n x (K-1)
    _P = _XR.T.dot(_v).dot(_XR)  # (K-1) x (K-1)
    _Omega = _R.dot(np.linalg.inv(_P).dot(_XR.T.dot(_v)))  # K x n
    _f = _Omega.dot(t_r)  # K x 1
    return _f, _Omega


def check_for_factor_return_colinear(t_r: np.ndarray, t_x: np.ndarray, t_instru_wgt: np.ndarray, t_factor_ret: np.ndarray):
    """

    :param t_r: same as the t_r in cal_risk_factor_return_colinear
    :param t_x: same as the t_x in cal_risk_factor_return_colinear
    :param t_instru_wgt: same as the t_instru_wgt in cal_risk_factor_return_colinear
    :param t_factor_ret: _f in cal_risk_factor_return_colinear
    :return:
    """
    _rh = t_x @ t_factor_ret
    _residual = t_r - _rh
    _w = np.sqrt(t_instru_wgt)
    _r_wgt_mean = t_r.dot(_w)
    _sst = np.sum((t_r - _r_wgt_mean) ** 2 * _w)
    _ssr = np.sum((_rh - _r_wgt_mean) ** 2 * _w)
    _sse = np.sum(_residual ** 2 * _w)
    _rsq = _ssr / _sst
    _err = np.abs(_sst - _ssr - _sse)
    return _residual, _sst, _ssr, _sse, _rsq, _err


def reg_one_day(reg_df: pd.DataFrame, x_lbls: list[str], y_lbl: str,
                weight_id: str, selected_sectors_list: list[str],
                fac_ret_dfs: list[pd.DataFrame], ins_res_dfs: list[pd.DataFrame], fac_ptf_dfs: list[pd.DataFrame],
                reg_square_data: list
                ):
    base_date, trade_date = reg_df["base_date"].iloc[0], reg_df["trade_date"].iloc[0]
    x, y = reg_df[x_lbls].values, reg_df[y_lbl].values
    n, k = x.shape
    if n <= k:
        print(base_date, trade_date, f"Dimension Error! n={n} < k={k}")
    else:
        instru_wgt = reg_df[weight_id] / reg_df[weight_id].sum()
        sector_wgt = reg_df[selected_sectors_list].T.dot(instru_wgt)
        factor_ret, factor_portfolio = cal_risk_factor_return_colinear(t_r=y, t_x=x, t_instru_wgt=instru_wgt.values, t_sector_wgt=sector_wgt.values)
        residual, sst, ssr, sse, rsq, err = check_for_factor_return_colinear(t_r=y, t_x=x, t_instru_wgt=instru_wgt, t_factor_ret=factor_ret)
        if err > 1e-6:
            print(base_date, trade_date, f"Regression Error! SST = {sst:.2f} SSR = {ssr:.2f} SSE = {sse:.2f} and SST != SSR + SSE")
        else:
            fac_ret_df = pd.DataFrame(data={"trade_date": trade_date, "factor": x_lbls, "return": factor_ret}).set_index("trade_date")
            ins_res_df = pd.DataFrame(data={"trade_date": trade_date, "instrument": reg_df["instrument"], "residual": residual}).set_index("trade_date")
            fac_ptf_df = pd.DataFrame(data=factor_portfolio, index=x_lbls, columns=reg_df["instrument"]).T
            fac_ptf_df["trade_date"] = trade_date
            fac_ptf_df = fac_ptf_df.reset_index().set_index("trade_date")
            fac_ret_dfs.append(fac_ret_df)
            ins_res_dfs.append(ins_res_df)
            fac_ptf_dfs.append(fac_ptf_df)
            reg_square_data.append(
                {
                    "trade_date": base_date,
                    "test_return_date": trade_date,
                    "num_instru": n,
                    "num_fac": k,
                    "num_mkt": 1,
                    "num_sec": len(selected_sectors_list),
                    "num_sty": k - 1 - len(selected_sectors_list),
                    "ssr": ssr,
                    "sse": sse,
                    "sst": sst,
                    "rsq": rsq,
                }
            )
    return 0


def factors_return(
        pid: str, selected_factors_pool: list[str], neutral_method: str, test_window: int, factors_return_lag: int,
        run_mode: str, bgn_date: str, stp_date: str | None,
        sectors: list[str], sector_classification: dict[str, str],
        concerned_instruments_universe: list[str],
        available_universe_dir: str, factors_exposure_delinear_dir: str, test_return_dir: str,
        factors_return_dir: str, factors_portfolio_dir: str, instruments_residual_dir: str,
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1]

):
    __test_lag = 1

    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    weight_id = "WGT{:02d}".format(test_window)

    # --- load delinearized factors
    delinear_lib_id = "{}.{}.DELINEAR".format(pid, neutral_method)
    delinear_lib_structure = database_structure[delinear_lib_id]
    delinear_lib = CManagerLibReader(t_db_name=delinear_lib_structure.m_lib_name, t_db_save_dir=factors_exposure_delinear_dir)
    delinear_lib.set_default(delinear_lib_structure.m_tab.m_table_name)

    # --- load test return
    test_return_lib_id = "test_return_{:03d}".format(test_window)
    test_return_lib_structure = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=test_return_dir)
    test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)

    # --- load sector df
    sector_df = pd.DataFrame.from_dict({z: {sector_classification[z]: 1} for z in concerned_instruments_universe}, orient="index").fillna(0)
    selected_sectors_list = [z for z in sectors if z in sector_df.columns]  # sector_df.columns may be a subset of sector_list and in random order
    sector_df["MARKET"] = 1.00

    # --- regression labels
    x_lbls = ["MARKET"] + selected_sectors_list + selected_factors_pool
    y_lbl = "value"

    # --- load available universe
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.set_default(available_universe_lib_structure.m_tab.m_table_name)

    # --- load calendar
    trade_calendar = CCalendar(t_path=calendar_path)
    shift_d = (__test_lag + test_window) * factors_return_lag
    iter_dates = trade_calendar.get_iter_list(bgn_date, stp_date, True)
    base_bgn_date = trade_calendar.get_next_date(iter_dates[0], -shift_d)
    base_stp_date = trade_calendar.get_next_date(iter_dates[-1], -shift_d + 1)
    iter_base_dates = trade_calendar.get_iter_list(base_bgn_date, base_stp_date, True)
    dates_bridge_df = pd.DataFrame({"trade_date": iter_dates, "base_date": iter_base_dates})

    test_return_df = test_return_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"]
    ).set_index(["trade_date", "instrument"])
    test_return_lib.close()

    available_universe_df = available_universe_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", weight_id]
    ).rename(mapper={"trade_date": "base_date"}, axis=1)
    available_universe_lib.close()

    delinearized_df = delinear_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument"] + selected_factors_pool
    ).rename(mapper={"trade_date": "base_date"}, axis=1)
    delinear_lib.close()

    sty_factor_exposure_df = pd.merge(left=available_universe_df, right=delinearized_df,
                                      on=["base_date", "instrument"], how="inner")
    all_factor_exposure_df = pd.merge(left=sector_df, right=sty_factor_exposure_df.set_index("instrument"),
                                      left_index=True, right_index=True, how="inner")
    all_factor_exposure_df_shift = pd.merge(left=dates_bridge_df, right=all_factor_exposure_df.reset_index(),
                                            on="base_date", how="right").rename(mapper={"index": "instrument"}, axis=1)
    all_reg_df = pd.merge(left=test_return_df, right=all_factor_exposure_df_shift,
                          on=["trade_date", "instrument"], how="inner")

    fac_ret_dfs, ins_res_dfs, fac_ptf_dfs = [], [], []
    reg_square_data = []
    all_reg_df.groupby(by="trade_date", group_keys=True).apply(reg_one_day, x_lbls=x_lbls, y_lbl=y_lbl,
                                                               weight_id=weight_id,
                                                               selected_sectors_list=selected_sectors_list,
                                                               fac_ret_dfs=fac_ret_dfs, ins_res_dfs=ins_res_dfs, fac_ptf_dfs=fac_ptf_dfs,
                                                               reg_square_data=reg_square_data
                                                               )
    update_fac_ret_df = pd.concat(fac_ret_dfs, axis=0, ignore_index=False).sort_index(ascending=True)
    update_ins_res_df = pd.concat(ins_res_dfs, axis=0, ignore_index=False).sort_index(ascending=True)
    update_fac_ptf_df = pd.concat(fac_ptf_dfs, axis=0, ignore_index=False).sort_index(ascending=True)

    # --- initialize output lib: factors_return/instruments_residual/factors_portfolio
    fac_ret_lib_id = "factors_return.{}.{}.TW{:03d}.T{}".format(pid, neutral_method, test_window, factors_return_lag)
    fac_ptf_lib_id = "factors_portfolio.{}.{}.TW{:03d}.T{}".format(pid, neutral_method, test_window, factors_return_lag)
    ins_res_lib_id = "instruments_residual.{}.{}.TW{:03d}.T{}".format(pid, neutral_method, test_window, factors_return_lag)

    fac_ret_lib = CManagerLibWriterByDate(t_db_save_dir=factors_return_dir, t_db_name=database_structure[fac_ret_lib_id].m_lib_name)
    fac_ptf_lib = CManagerLibWriterByDate(t_db_save_dir=factors_portfolio_dir, t_db_name=database_structure[fac_ptf_lib_id].m_lib_name)
    ins_res_lib = CManagerLibWriterByDate(t_db_save_dir=instruments_residual_dir, t_db_name=database_structure[ins_res_lib_id].m_lib_name)

    fac_ret_lib.initialize_table(t_table=database_structure[fac_ret_lib_id].m_tab, t_remove_existence=run_mode in ["O"])
    fac_ptf_lib.initialize_table(t_table=database_structure[fac_ptf_lib_id].m_tab, t_remove_existence=run_mode in ["O"])
    ins_res_lib.initialize_table(t_table=database_structure[ins_res_lib_id].m_tab, t_remove_existence=run_mode in ["O"])

    fac_ret_lib.update(t_update_df=update_fac_ret_df, t_using_index=True)
    fac_ptf_lib.update(t_update_df=update_fac_ptf_df, t_using_index=True)
    ins_res_lib.update(t_update_df=update_ins_res_df, t_using_index=True)

    # close all libs
    fac_ret_lib.close()
    fac_ptf_lib.close()
    ins_res_lib.close()

    # save regression
    reg_square_file = "reg_square.{}.{}.TW{:03d}.T{}.csv.gz".format(pid, neutral_method, test_window, factors_return_lag)
    reg_square_path = os.path.join(factors_return_dir, reg_square_file)
    new_reg_square_df = pd.DataFrame(reg_square_data)
    if run_mode in ["O", "OVERWRITE"]:
        reg_square_df = new_reg_square_df
    else:
        old_reg_square_df = pd.read_csv(reg_square_path, dtype={"trade_date": str})
        reg_square_df = pd.concat([old_reg_square_df, new_reg_square_df]).drop_duplicates(subset=["trade_date"])
    reg_square_df.to_csv(reg_square_path, index=False, float_format="%.6f")
    plot_bar(
        t_bar_df=reg_square_df.set_index("test_return_date")[["rsq"]],
        t_stacked=False, t_fig_name=reg_square_file.replace(".csv.gz", ""),
        t_xtick_span=63, t_tick_label_rotation=90,
        t_save_dir=factors_return_dir
    )
    return 0


def cal_factors_return_mp(proc_num: int,
                          pids: list[str], factors_pool_options, neutral_methods: list[str],
                          test_windows: list[int], factors_return_lags: list[int],
                          **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p, nm, tw, lag in itertools.product(pids, neutral_methods, test_windows, factors_return_lags):
        selected_factors_pool = factors_pool_options[p]
        pool.apply_async(factors_return, args=(p, selected_factors_pool, nm, tw, lag),
                         kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
