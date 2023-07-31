import os
import numpy as np
import pandas as pd
import scipy.stats as sps
from itertools import product
from skyrim.falkreath import CLib1Tab1, CManagerLibReader


# -------------------------------------------
# --- Part I: factor exposure calculation ---


def cal_rolling_corr(t_major_return_df: pd.DataFrame, t_x: str, t_y: str, t_rolling_window: int, t_corr_lbl: str):
    t_major_return_df["xy"] = (t_major_return_df[t_x] * t_major_return_df[t_y]).rolling(window=t_rolling_window).mean()
    t_major_return_df["xx"] = (t_major_return_df[t_x] * t_major_return_df[t_x]).rolling(window=t_rolling_window).mean()
    t_major_return_df["yy"] = (t_major_return_df[t_y] * t_major_return_df[t_y]).rolling(window=t_rolling_window).mean()
    t_major_return_df["x"] = t_major_return_df[t_x].rolling(window=t_rolling_window).mean()
    t_major_return_df["y"] = t_major_return_df[t_y].rolling(window=t_rolling_window).mean()

    t_major_return_df["cov_xy"] = t_major_return_df["xy"] - t_major_return_df["x"] * t_major_return_df["y"]
    t_major_return_df["cov_xx"] = t_major_return_df["xx"] - t_major_return_df["x"] * t_major_return_df["x"]
    t_major_return_df["cov_yy"] = t_major_return_df["yy"] - t_major_return_df["y"] * t_major_return_df["y"]

    t_major_return_df.loc[np.abs(t_major_return_df["cov_xx"]) <= 1e-8, "cov_xx"] = 0
    t_major_return_df.loc[np.abs(t_major_return_df["cov_yy"]) <= 1e-8, "cov_yy"] = 0

    t_major_return_df["sqrt_cov_xx_yy"] = np.sqrt(t_major_return_df["cov_xx"] * t_major_return_df["cov_yy"]).fillna(0)
    t_major_return_df[t_corr_lbl] = t_major_return_df[["cov_xy", "sqrt_cov_xx_yy"]].apply(
        lambda z: 0 if z["sqrt_cov_xx_yy"] == 0 else -z["cov_xy"] / z["sqrt_cov_xx_yy"], axis=1)
    return 0


def transform_dist(t_exposure_srs: pd.Series):
    """

    :param t_exposure_srs:
    :return: remap an array of any distribution to the union distribution
    """

    return sps.norm.ppf(t_exposure_srs.rank() / (len(t_exposure_srs) + 1))


def neutralize_by_sector(t_raw_data: pd.Series, t_sector_df: pd.DataFrame, t_weight=None):
    """

    :param t_raw_data: A pd.Series with length = N. Its value could be exposure or return.
    :param t_sector_df: A 0-1 matrix with size = (M, K). And M >=N, make sure that index of
                        t_raw_data is a subset of the index of t_sector_df before call this
                        function.
                        Element[m, k] = 1 if Instruments[m] is in Sector[k] else 0.
    :param t_weight: A pd.Series with length = N1 >= N, make sure that index of t_raw_data
                     is a subset of the index of t_weight. Each element means relative weight
                     of corresponding instrument when regression.
    :return:
    """
    n = len(t_raw_data)
    idx = t_raw_data.index
    if t_weight is None:
        _w: np.ndarray = np.ones(n) / n
    else:
        _w: np.ndarray = t_weight[idx].values

    _w = np.diag(_w)  # It is allowed that sum of _w may not equal 1
    _x: np.ndarray = t_sector_df.loc[idx].values
    _y: np.ndarray = t_raw_data.values

    xw = _x.T.dot(_w)
    xwx_inv = np.linalg.inv(xw.dot(_x))
    xwy = xw.dot(_y)
    b = xwx_inv.dot(xwy)  # b = [XWX]^{-1}[XWY]
    _r = _y - _x.dot(b)  # r = Y - bX
    return pd.Series(data=_r, index=idx)


def load_factor_return(test_window: int,
                       pid: str, neutral_method: str, factors_return_lag: int,
                       loading_factors: list[str],
                       factors_return_dir: str,
                       database_structure: dict[str, CLib1Tab1]):
    # --- load lib: factors_return/instruments_residual
    factors_return_lib_id = "factors_return.{}.{}.TW{:03d}.T{}".format(pid, neutral_method, test_window, factors_return_lag)
    factors_return_lib = CManagerLibReader(t_db_save_dir=factors_return_dir, t_db_name=database_structure[factors_return_lib_id].m_lib_name)
    factors_return_lib.set_default(database_structure[factors_return_lib_id].m_tab.m_table_name)
    factors_return_df = factors_return_lib.read(t_value_columns=["trade_date", "factor", "value"])
    factors_return_lib.close()

    factors_return_agg_df = pd.pivot_table(data=factors_return_df, index="trade_date", columns="factor", values="value", aggfunc=sum)
    factors_return_agg_df = factors_return_agg_df.sort_index(ascending=True)
    factors_return_agg_df = factors_return_agg_df[loading_factors]
    return factors_return_agg_df


if __name__ == "__main__":
    # ---- TEST EXAMPLE 0
    print("---- TEST EXAMPLE 0")
    sector_classification = {
        "CU.SHF": "METAL",
        "AL.SHF": "METAL",
        "ZN.SHF": "METAL",
        "A.DCE": "OIL",
        "M.DCE": "OIL",
        "Y.DCE": "OIL",
        "P.DCE": "OIL",
        "MA.CZC": "CHEM",
        "TA.CZC": "CHEM",
        "PP.DCE": "CHEM",
    }

    sector_df = pd.DataFrame.from_dict({z: {sector_classification[z]: 1} for z in sector_classification}, orient="index").fillna(0)
    print(sector_df)

    raw_factor = pd.Series({
        "CU.SHF": 10,
        "ZN.SHF": 8,
        "Y.DCE": 3,
        "P.DCE": 0,
        "MA.CZC": -2,
        "TA.CZC": -4,
    })

    weight = pd.Series({
        "Y.DCE": 2,
        "P.DCE": 1,
        "MA.CZC": 1,
        "TA.CZC": 1,
        "CU.SHF": 1,
        "ZN.SHF": 2,
    })

    new_factor = neutralize_by_sector(
        t_raw_data=raw_factor,
        t_sector_df=sector_df,
        t_weight=weight,
    )

    df = pd.DataFrame({
        "OLD": raw_factor,
        "WGT": weight,
        "NEW": new_factor,
    }).loc[raw_factor.index]

    print(df)

    # ---- TEST EXAMPLE 1
    print("---- TEST EXAMPLE 1")
    df = pd.DataFrame({
        "sector": ["I1", "I1", "I2", "I2"],
        "I1": [1, 1, 0, 0],
        "I2": [0, 0, 1, 1],
        "raw_factor": [100, 80, 32, 8],
        "raw_return": [24, 6, 45, 15],
        "ave_factor_by_sec": [90, 90, 20, 20],
        "ave_return_by_sec": [15, 15, 30, 30],
    }, index=["S1", "S2", "S3", "S4"])
    df.index.name = "资产"
    df["neu_factor"] = neutralize_by_sector(t_raw_data=df["raw_factor"], t_sector_df=df[["I1", "I2"]], t_weight=None)
    df["neu_return"] = neutralize_by_sector(t_raw_data=df["raw_return"], t_sector_df=df[["I1", "I2"]], t_weight=None)

    # wgt_srs = pd.Series({"S1": 1, "S2": 1, "S3": 2, "S4": 3})
    # df["neu_factor"] = neutralize_by_sector(t_raw_data=df["raw_factor"], t_sector_df=df[["I1", "I2"]], t_weight=wgt_srs)
    # df["neu_return"] = neutralize_by_sector(t_raw_data=df["raw_return"], t_sector_df=df[["I1", "I2"]], t_weight=wgt_srs)

    pd.set_option("display.width", 0)
    print(df)

    for x, y in product(["raw_factor", "neu_factor", "I1", "I2"], ["raw_return", "neu_return"]):
        r = df[[x, y]].corr().loc[x, y]
        # print("Corr({:4s},{:4s}) = {:>9.4f}".format(x, y, r))
        print("{} & {} & {:>.3f}\\\\".format(x, y, r))
