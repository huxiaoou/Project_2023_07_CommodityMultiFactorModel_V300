import os
import numpy as np
import datetime as dt
import pandas as pd
from skyrim.whiterun import SetFontGreen
from skyrim.falkreath import CLib1Tab1, CManagerLibReader


def cal_market_return(instruments_return_dir: str,
                      available_universe_dir: str,
                      database_structure: dict[str, CLib1Tab1]):
    # --- load
    return_file = "instruments.return.csv.gz"
    return_path = os.path.join(instruments_return_dir, return_file)
    return_df = pd.read_csv(return_path, dtype={"trade_date": str}).set_index("trade_date")

    # --- initialize lib
    available_universe_lib_structure = database_structure["available_universe"]
    available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=available_universe_dir)
    available_universe_lib.set_default(t_default_table_name=available_universe_lib_structure.m_tab.m_table_name)

    # --- loop
    market_index_return_data = []
    for trade_date in return_df.index:
        available_universe_df = available_universe_lib.read_by_date(
            t_value_columns=["instrument", "return", "amount"],
            t_trade_date=trade_date
        )
        if len(available_universe_df) == 0:
            continue

        available_universe_df["rel_wgt"] = np.sqrt(available_universe_df["amount"])  # = 1 if you want weight of instruments are equal
        available_universe_df["wgt"] = available_universe_df["rel_wgt"] / available_universe_df["rel_wgt"].sum()
        mkt_idx_trade_date_ret = available_universe_df["wgt"].dot(available_universe_df["return"])
        market_index_return_data.append({
            "trade_date": trade_date,
            "market": mkt_idx_trade_date_ret
        })
    available_universe_lib.close()

    market_index_df = pd.DataFrame(market_index_return_data).set_index("trade_date")
    market_index_file = "market.return.csv.gz"
    market_index_path = os.path.join(instruments_return_dir, market_index_file)
    market_index_df.to_csv(market_index_path, float_format="%.10f")

    print(market_index_df)
    print("... @ {} {} calculated".format(dt.datetime.now(), SetFontGreen('market index return')))
    return 0
