import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CManagerLibReader, CManagerLibWriter, CLib1Tab1
from factors.factors_shared import transform_dist, neutralize_by_sector


def drop_df_rows_by_nan_prop(t_df: pd.DataFrame, t_factors_list: list, t_nan_prop_threshold: float = 1 - 0.618):
    """

    :param t_df:
    :param t_factors_list:
    :param t_nan_prop_threshold: must be [0, 1], keep rows with the nan ratios less than this threshold
    :return:
    """

    nan_prop = (t_df[t_factors_list].isnull().sum(axis=1) / len(t_factors_list))
    f = nan_prop <= t_nan_prop_threshold
    selected_df: pd.DataFrame = t_df.loc[f]
    m = selected_df.median()
    selected_df_fillna = selected_df.fillna(m)
    return selected_df_fillna


def adjust_weight(t_input_df: pd.DataFrame, t_weight_id: str, t_neutral_method: str):
    # update weight_id according to neutral method
    if t_neutral_method == "WE":
        t_input_df["rel_w"] = 1
    elif t_neutral_method == "WS":
        t_input_df["rel_w"] = np.sqrt(t_input_df[t_weight_id])
    elif t_neutral_method == "WA":
        t_input_df["rel_w"] = t_input_df[t_weight_id]
    t_input_df["w"] = t_input_df["rel_w"] / t_input_df["rel_w"].sum()
    return 0


def sector_neutralize_factors_pool(t_input_df: pd.DataFrame, t_sectors_list: list, t_factors_list: list, t_weight_id: str = "w"):
    sector_neutralized_data = {}
    for factor in t_factors_list:
        sector_neutralized_data[factor] = neutralize_by_sector(
            t_raw_data=t_input_df[factor],
            t_sector_df=t_input_df[t_sectors_list],
            t_weight=t_input_df[t_weight_id]
        )
    return pd.DataFrame(sector_neutralized_data)


def normalize(t_sector_neutralized_df: pd.DataFrame, w: pd.Series):
    m = t_sector_neutralized_df.T @ w
    s = np.sqrt(((t_sector_neutralized_df - m) ** 2).T @ w)
    return (t_sector_neutralized_df - m) / s


def delinear(t_exposure_df: pd.DataFrame, t_selected_factors_pool: list, w: pd.Series) -> pd.DataFrame:
    delinear_exposure_data = {}
    f_h_square_sum = {}
    for i, fi_lbl in enumerate(t_selected_factors_pool):
        if i == 0:
            delinear_exposure_data[fi_lbl] = t_exposure_df[fi_lbl]
        else:  # i > 0, since the second component
            fi = t_exposure_df[fi_lbl]
            projection = 0
            for j, fj_lbl in zip(range(i), t_selected_factors_pool[0:i]):
                fj_h = delinear_exposure_data[fj_lbl]
                projection += fi.dot(w * fj_h) / f_h_square_sum[fj_lbl] * fj_h
            delinear_exposure_data[fi_lbl] = fi - projection
        f_h_square_sum[fi_lbl] = delinear_exposure_data[fi_lbl].dot(w * delinear_exposure_data[fi_lbl])
    delinear_exposure_df = pd.DataFrame(delinear_exposure_data)
    res_df = normalize(delinear_exposure_df, w)
    return res_df


def norm_and_delinear_one_day(df: pd.DataFrame,
                              weight_id: str, neutral_method: str,
                              selected_factors_pool: list[str], selected_sectors_list: list[str],
                              norm_dfs: list, deln_dfs: list,
                              validate: bool = False):
    """

    :param df: columns = ["trade_date", weight_id] + sector_exposures + style_exposures
    :param weight_id:
    :param neutral_method:
    :param selected_factors_pool:
    :param selected_sectors_list:
    :param norm_dfs:
    :param deln_dfs:
    :param validate:
    :return:
    """
    trade_date = df["trade_date"].iloc[0]

    # Drop rows with too many nan
    # After this state, no rows are dropped.
    raw_input_df = drop_df_rows_by_nan_prop(t_df=df.drop(axis=1, labels=["trade_date"]), t_factors_list=selected_factors_pool)

    # Transform general factors distribution
    raw_input_df[selected_factors_pool] = raw_input_df[selected_factors_pool].apply(transform_dist)

    # Set weight
    adjust_weight(t_input_df=raw_input_df, t_weight_id=weight_id, t_neutral_method=neutral_method)

    # Get sector neutralized exposure
    sector_neutralized_df = sector_neutralize_factors_pool(
        t_input_df=raw_input_df,
        t_factors_list=selected_factors_pool, t_sectors_list=selected_sectors_list)

    # Normalize and Delinear
    norm_factors_df = normalize(t_sector_neutralized_df=sector_neutralized_df, w=raw_input_df["w"])
    delinearized_df = delinear(t_exposure_df=norm_factors_df, t_selected_factors_pool=selected_factors_pool, w=raw_input_df["w"])

    if validate:
        xdf = pd.merge(
            left=raw_input_df[selected_sectors_list],
            right=delinearized_df,
            left_index=True, right_index=True,
            how="outer")
        ws = raw_input_df["w"]
        wdf = pd.DataFrame(data=np.diag(ws), index=ws.index, columns=ws.index)
        m, v = xdf.T @ raw_input_df["w"], np.round(xdf.T @ wdf @ xdf, 8)
        print(m, v)

    norm_factors_df["trade_date"] = trade_date
    delinearized_df["trade_date"] = trade_date
    norm_factors_df_save = norm_factors_df.reset_index().set_index("trade_date")
    delinearized_df_save = delinearized_df.reset_index().set_index("trade_date")

    # norm_factors_df_save = norm_factors_df.set_index("trade_date").swaplevel()
    # delinearized_df_save = delinearized_df.set_index("trade_date").swaplevel()
    norm_dfs.append(norm_factors_df_save)
    deln_dfs.append(delinearized_df_save)
    return 0


def factors_normalize_and_delinear(
        pid: str, selected_factors_pool: list[str],
        neutral_method: str,
        run_mode: str, bgn_date: str, stp_date: str,
        concerned_instruments_universe: list[str],
        sector_classification: dict[str, str],
        available_universe_dir: str,
        factors_exposure_dir: str,
        factors_exposure_norm_dir: str,
        factors_exposure_delinear_dir: str,
        database_structure: dict[str, CLib1Tab1]
):
    __weight_id = "amount"

    # set dates
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load available universe
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.set_default(available_universe_lib_structure.m_tab.m_table_name)
    available_universe_df = available_universe_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", __weight_id])
    available_universe_df = available_universe_df.set_index(["trade_date", "instrument"])
    available_universe_lib.close()

    # --- load sector exposures
    sector_df = pd.DataFrame.from_dict({z: {sector_classification[z]: 1} for z in concerned_instruments_universe}, orient="index").fillna(0)
    selected_sectors_list = sector_df.columns.to_list()

    # --- load raw factor exposures
    factors_exposure_data = {}
    for factor in selected_factors_pool:
        factor_lib_structure = database_structure[factor]
        factor_lib = CManagerLibReader(t_db_name=factor_lib_structure.m_lib_name, t_db_save_dir=factors_exposure_dir)
        factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)
        factor_exposure_df = factor_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"])
        factors_exposure_data[factor] = factor_exposure_df.set_index(["trade_date", "instrument"])["value"]
        factor_lib.close()
    raw_factors_exposure_df = pd.DataFrame(factors_exposure_data)

    # --- core
    sty_factor_exposure_df = pd.merge(left=available_universe_df, right=raw_factors_exposure_df,
                                      left_index=True, right_index=True, how="left")
    sty_factor_exposure_df.reset_index(level=0, inplace=True)
    all_factor_exposure_df = pd.merge(left=sector_df, right=sty_factor_exposure_df,
                                      left_index=True, right_index=True, how="inner")
    all_factor_exposure_df.sort_values("trade_date", inplace=True)

    norm_dfs, deln_dfs = [], []
    all_factor_exposure_df.groupby(by="trade_date", group_keys=True).apply(
        norm_and_delinear_one_day, weight_id=__weight_id, neutral_method=neutral_method,
        selected_factors_pool=selected_factors_pool, selected_sectors_list=selected_sectors_list,
        norm_dfs=norm_dfs, deln_dfs=deln_dfs,
    )
    update_df_norm = pd.concat(norm_dfs, axis=0, ignore_index=False)
    update_df_deln = pd.concat(deln_dfs, axis=0, ignore_index=False)

    # --- initialize output factor lib
    norm_lib_id = "{}.{}.NORM".format(pid, neutral_method)
    deln_lib_id = "{}.{}.DELINEAR".format(pid, neutral_method)
    norm_lib = CManagerLibWriter(t_db_save_dir=factors_exposure_norm_dir, t_db_name=database_structure[norm_lib_id].m_lib_name)
    deln_lib = CManagerLibWriter(t_db_save_dir=factors_exposure_delinear_dir, t_db_name=database_structure[deln_lib_id].m_lib_name)
    norm_lib.initialize_table(t_table=database_structure[norm_lib_id].m_tab, t_remove_existence=run_mode in ["O"])
    deln_lib.initialize_table(t_table=database_structure[deln_lib_id].m_tab, t_remove_existence=run_mode in ["O"])
    norm_lib.update(t_update_df=update_df_norm, t_using_index=True)
    deln_lib.update(t_update_df=update_df_deln, t_using_index=True)

    # --- close libs
    norm_lib.close()
    deln_lib.close()
    return 0


def cal_factors_normalize_and_delinear_mp(proc_num: int, pids: list[int], **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p in pids:
        pool.apply_async(factors_normalize_and_delinear, args=(p,), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... factors DELINEAR calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
