import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from factors.factors_shared import transform_dist, neutralize_by_sector
from skyrim.falkreath import CManagerLibReader, CManagerLibWriter, CLib1Tab1


def neutralize_one_factor_one_day(
        df: pd.DataFrame, mother_df: pd.DataFrame, neutral_method: str,
        weight_id: str, sector_df: pd.DataFrame) -> pd.Series:
    xdf = pd.merge(left=mother_df, right=df, on="instrument", how="inner").set_index("instrument")
    xdf["value_norm"] = transform_dist(t_exposure_srs=xdf["value"])
    if neutral_method == "WE":
        xdf["rel_wgt"] = 1
    elif neutral_method == "WS":
        xdf["rel_wgt"] = np.sqrt(xdf[weight_id])
    factor_neutral_srs = neutralize_by_sector(
        t_raw_data=xdf["value_norm"],
        t_sector_df=sector_df,
        t_weight=xdf["rel_wgt"]
    )
    return factor_neutral_srs


def neutralize_one_factor(factor: str,
                          neutral_method: str,
                          run_mode: str, bgn_date: str, stp_date: str,
                          concerned_instruments_universe: list[str],
                          sector_classification: dict[str, str],
                          available_universe_dir: str,
                          factors_exposure_dir: str,
                          factors_exposure_neutral_dir: str,
                          database_structure: dict[str, CLib1Tab1],
                          ):
    __weight_id = "amount"
    # --- calendar
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- available universe
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.set_default(available_universe_lib_structure.m_tab.m_table_name)

    # --- mother universe
    mother_universe_df = pd.DataFrame({"instrument": concerned_instruments_universe})

    # --- sector df
    sector_df = pd.DataFrame.from_dict({z: {sector_classification[z]: 1} for z in concerned_instruments_universe}, orient="index").fillna(0)

    # --- factor library
    factor_lib_structure = database_structure[factor]
    factor_lib = CManagerLibReader(t_db_name=factor_lib_structure.m_lib_name, t_db_save_dir=factors_exposure_dir)
    factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)

    # --- factor neutral library
    factor_neutral_lib_id = "{}.{}".format(factor, neutral_method)
    factor_neutral_lib_structure = database_structure[factor_neutral_lib_id]
    factor_neutral_lib = CManagerLibWriter(t_db_name=factor_neutral_lib_structure.m_lib_name, t_db_save_dir=factors_exposure_neutral_dir)
    factor_neutral_lib.initialize_table(t_table=factor_neutral_lib_structure.m_tab, t_remove_existence=run_mode in ["O"])

    factor_df = factor_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])
    factor_lib.close()

    weight_df = available_universe_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", __weight_id])
    available_universe_lib.close()

    input_df = pd.merge(left=factor_df, right=weight_df, on=["trade_date", "instrument"], how="inner")
    res_agg = input_df.groupby(by="trade_date", group_keys=True).apply(
        neutralize_one_factor_one_day, mother_df=mother_universe_df,
        neutral_method=neutral_method, weight_id=__weight_id, sector_df=sector_df,
    )

    # type of res_agg may vary according to the result:
    # if length of each day is the same, it will be a DataFrame(this happens when only a few days are calculated)
    # otherwise it would be a DataFrame(this happens when all days in history are calculated)

    if type(res_agg) == pd.Series:
        update_df = res_agg.reset_index()
    elif type(res_agg) == pd.DataFrame:
        update_df = res_agg.stack().reset_index()
    else:
        print("... Wrong type of result when calculate factors neutral.")
        print("... The result is neither a pd.Series nor a pd.DataFrame.")
        update_df = pd.DataFrame()
    factor_neutral_lib.update(t_update_df=update_df, t_using_index=False)
    factor_neutral_lib.close()
    return 0


def cal_factors_neutral_mp(
        proc_num: int,
        factors: list[str],
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for f in factors:
        pool.apply_async(neutralize_one_factor, args=(f,), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... factors NEUTRAL calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
