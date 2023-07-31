import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.winterhold import check_and_mkdir
from skyrim.whiterun import CCalendar, CInstrumentInfoTable
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.riverwood2 import CManagerMarketData


def load_positions(sig_id: str, sig_date: str,
                   signals_opt_dir: str,
                   database_structure: dict[str, CLib1Tab1]) -> pd.DataFrame:
    # --- init allocation opt lib
    sig_opt_lib = CManagerLibReader(t_db_save_dir=signals_opt_dir, t_db_name=database_structure[sig_id].m_lib_name)
    sig_opt_lib.set_default(database_structure[sig_id].m_tab.m_table_name)
    sig_df = sig_opt_lib.read_by_date(t_trade_date=sig_date, t_value_columns=["instrument", "value"])
    sig_opt_lib.close()
    return sig_df


class CCalculatorPosAndTrades(object):
    def __init__(self,
                 instruments_universe: list[str],
                 md_by_instru_dir: str,
                 major_minor_dir: str,
                 simu_positions_and_trades_dir: str,
                 calendar_path: str,
                 instru_info_tab_path: str,
                 ):
        self.m_instruments = instruments_universe
        self.m_calendar = CCalendar(calendar_path)
        self.m_instru_info_tab = CInstrumentInfoTable(instru_info_tab_path, t_index_label="windCode", t_type="CSV")
        self.m_mmd = CManagerMarketData(instruments_universe, md_by_instru_dir, major_minor_dir)
        self.m_simu_positions_and_trades_dir = simu_positions_and_trades_dir

    def __match_major_contract(self, sig_df: pd.DataFrame, trade_date: str):
        """

        :param sig_df: columns = ["instrument", "value"]
        :param trade_date:
        :return:
        """
        sig_df["contract"] = sig_df.apply(
            lambda z: self.m_mmd.inquiry_major_contract(z["instrument"], trade_date), axis=1)
        return 0

    def __match_price(self, sig_df: pd.DataFrame, trade_date: str, date_tag: str, prc_type: str):
        """

        :param sig_df: columns = ["instrument", "value"]
        :param trade_date:
        :param date_tag:
        :param prc_type:
        :return:
        """
        sig_df[f"{prc_type}_{date_tag}"] = sig_df.apply(
            lambda z: self.m_mmd.inquiry_price_at_date(z["contract"], z["instrument"], trade_date, prc_type), axis=1)
        return 0

    def __match_instrument_contract_multiplier(self, sig_df: pd.DataFrame):
        sig_df["contractMultiplier"] = sig_df["instrument"].map(lambda z: self.m_instru_info_tab.get_multiplier(z))
        return 0

    @staticmethod
    def __estimate_direction_and_quantity(sig_df: pd.DataFrame, available_amount: float, date_tag: str, prc_type: str):
        prc_lbl = f"{prc_type}_{date_tag}"
        sig_df["direction"] = sig_df["value"].map(lambda z: 1 if z >= 0 else -1)
        sig_df["quantity"] = sig_df.apply(
            lambda z: int(np.round(abs(z["value"]) * available_amount / z[prc_lbl] / z["contractMultiplier"])), axis=1)
        return 0

    @staticmethod
    def __estimate_market_value_and_prop(sig_df: pd.DataFrame, available_amount: float, date_tag: str, prc_type: str):
        prc_lbl = f"{prc_type}_{date_tag}"
        sig_df["marketValue"] = sig_df[prc_lbl] * sig_df["contractMultiplier"] * sig_df["quantity"] / 1e4
        sig_df["marketValueProp"] = sig_df["marketValue"] / available_amount * 1e4
        return 0

    def cal_simu_positions(self, sig_id: str, exe_date: str,
                           available_amount: float,
                           signals_opt_dir: str,
                           database_structure: dict[str, CLib1Tab1]
                           ):
        sig_date = self.m_calendar.get_next_date(exe_date, -1)

        sig_df = load_positions(sig_id, sig_date, signals_opt_dir, database_structure)
        self.__match_major_contract(sig_df, trade_date=sig_date)
        self.__match_price(sig_df, trade_date=sig_date, date_tag="sig", prc_type="close")
        self.__match_price(sig_df, trade_date=exe_date, date_tag="exe", prc_type="close")
        self.__match_instrument_contract_multiplier(sig_df)
        CCalculatorPosAndTrades.__estimate_direction_and_quantity(sig_df, available_amount=available_amount, date_tag="sig", prc_type="close")
        CCalculatorPosAndTrades.__estimate_market_value_and_prop(sig_df, available_amount=available_amount, date_tag="exe", prc_type="close")
        pd.set_option("display.width", 0)
        check_and_mkdir(year_dir := os.path.join(self.m_simu_positions_and_trades_dir, exe_date[0:4]))
        check_and_mkdir(date_dir := os.path.join(year_dir, exe_date))
        sig_file = f"simu-sig_{sig_date}-exe_{exe_date}-{sig_id}.csv.gz"
        sig_path = os.path.join(date_dir, sig_file)
        sig_df.to_csv(sig_path, float_format="%.6f", index=False)
        return 0

    def __load_sig_df(self, sig_id: str, sig_date: str, exe_date: str) -> pd.DataFrame:
        sig_file = f"simu-sig_{sig_date}-exe_{exe_date}-{sig_id}.csv.gz"
        date_dir = os.path.join(self.m_simu_positions_and_trades_dir, exe_date[0:4], exe_date)
        sig_path = os.path.join(date_dir, sig_file)
        try:
            return pd.read_csv(sig_path)
        except FileNotFoundError:
            print(f"... signal position data for {sig_id} with sig_date = {sig_date}, exe_date = {exe_date} are not found")
            print("... Program fails to continue, please check again.")
            return pd.DataFrame()

    def __get_merge_pos_df(self, sig_id: str, prev_sig_date: str, prev_exe_date: str, this_sig_date: str, this_exe_date: str) -> pd.DataFrame:
        prev_sig_df = self.__load_sig_df(sig_id, prev_sig_date, prev_exe_date)
        this_sig_df = self.__load_sig_df(sig_id, this_sig_date, this_exe_date)
        if prev_sig_df.empty:
            return pd.DataFrame()
        if this_sig_df.empty:
            return pd.DataFrame()
        merge_pos_df = pd.merge(
            left=prev_sig_df[["instrument", "contract", "direction", "contractMultiplier", "quantity"]],
            right=this_sig_df[["instrument", "contract", "direction", "contractMultiplier", "quantity", "value", "marketValue", "marketValueProp"]],
            on=["instrument", "contract", "direction", "contractMultiplier"], how="outer", suffixes=("Prev", "This")
        )
        merge_pos_df.fillna(0, inplace=True)
        merge_pos_df["quantityDiff"] = merge_pos_df["quantityThis"] - merge_pos_df["quantityPrev"]
        merge_pos_df["exePrc"] = merge_pos_df.apply(
            lambda z: self.m_mmd.inquiry_price_at_date(z["contract"], z["instrument"], this_exe_date, "close"), axis=1)
        return merge_pos_df

    def __save_merge_pos_df(self, merge_pos_df: pd.DataFrame, sig_id: str, this_sig_date: str, this_exe_date: str):
        # format adjust
        merge_pos_df[["quantityPrev", "quantityThis", "quantityDiff"]] = merge_pos_df[["quantityPrev", "quantityThis", "quantityDiff"]].astype(int)
        merge_pos_df["marketValue"] = merge_pos_df["marketValue"].map(lambda _: f"{_:.2f}")
        merge_pos_df["exePrc"] = merge_pos_df["exePrc"].map(lambda _: f"{_:.2f}")

        # save
        merge_pos_file = f"merged-sig_{this_sig_date}-exe_{this_exe_date}-{sig_id}.csv"
        merge_pos_path = os.path.join(self.m_simu_positions_and_trades_dir, this_exe_date[0:4], this_exe_date, merge_pos_file)
        merge_pos_df.to_csv(merge_pos_path, index=False, float_format="%.6f")
        return 0

    def __save_trades_df(self, trades_df: pd.DataFrame, sig_id: str, this_sig_date: str, this_exe_date: str):
        # format adjust
        trades_file = f"trades-sig_{this_sig_date}-exe_{this_exe_date}-{sig_id}.csv"
        trades_path = os.path.join(self.m_simu_positions_and_trades_dir, this_exe_date[0:4], this_exe_date, trades_file)
        trades_df.to_csv(trades_path, index=False, float_format="%.2f")
        return 0

    def __save_pnl_df(self, pnl_df: pd.DataFrame, sig_id: str, open_date: str, close_date: str):
        # format adjust
        pnl_file = f"pnl-openDate_{open_date}-closeDate_{close_date}-{sig_id}.csv"
        pnl_path = os.path.join(self.m_simu_positions_and_trades_dir, close_date[0:4], close_date, pnl_file)
        pnl_df.to_csv(pnl_path, index=False, float_format="%.4f")
        return 0

    def cal_simu_trades(self, sig_id: str, exe_date: str, verbose: bool = False):
        this_sig_date = self.m_calendar.get_next_date(exe_date, -1)
        prev_sig_date = self.m_calendar.get_next_date(exe_date, -2)
        prev_exe_date, this_exe_date = this_sig_date, exe_date
        merge_pos_df = self.__get_merge_pos_df(sig_id,
                                               prev_sig_date=prev_sig_date, prev_exe_date=prev_exe_date,
                                               this_sig_date=this_sig_date, this_exe_date=this_exe_date)
        if merge_pos_df.empty:
            return 0
        trades_data = []
        for iter_tuple in merge_pos_df.itertuples():
            contract, direction = getattr(iter_tuple, "contract"), getattr(iter_tuple, "direction")
            qty_diff, exe_prc = getattr(iter_tuple, "quantityDiff"), getattr(iter_tuple, "exePrc")
            contract_multiplier = getattr(iter_tuple, "contractMultiplier")
            amt = abs(qty_diff) * exe_prc * contract_multiplier
            if qty_diff != 0:
                trades_data.append({
                    "contract": contract,
                    "direction": direction,
                    "operation": "open" if qty_diff > 0 else "close",
                    "quantity": int(abs(qty_diff)),
                    "exePrice": exe_prc,
                    "amt": amt,
                })
        trades_df = pd.DataFrame(trades_data)
        trades_df.sort_values(by=["direction", "operation"], ascending=[False, True], inplace=True)

        # --- extra info
        market_exposure = merge_pos_df["marketValue"] @ merge_pos_df["direction"]
        market_value_sum = merge_pos_df["marketValue"].abs().sum()
        trades_summary_df = pd.pivot_table(data=trades_df, index="direction", columns="operation", values="amt", aggfunc=sum) / 1e4
        if verbose:
            print(trades_summary_df)

        self.__save_merge_pos_df(merge_pos_df, sig_id=sig_id, this_sig_date=this_sig_date, this_exe_date=this_exe_date)
        self.__save_trades_df(trades_df, sig_id, this_sig_date, this_exe_date)
        print(f"... {sig_id} @ {exe_date} market exposure  = {market_exposure:10.2f}, market value sum = {market_value_sum:10.2f}")
        return 0

    def cal_simu_pnl(self, sig_id: str, exe_date: str, available_amount: float):
        prev_sig_date = self.m_calendar.get_next_date(exe_date, -2)
        prev_exe_date = self.m_calendar.get_next_date(exe_date, -1)
        open_date = prev_exe_date[0:4] + "-" + prev_exe_date[4:6] + "-" + prev_exe_date[6:8]
        close_date = exe_date[0:4] + "-" + exe_date[4:6] + "-" + exe_date[6:8]
        sig_df = self.__load_sig_df(sig_id, prev_sig_date, prev_exe_date)
        self.__match_price(sig_df, trade_date=exe_date, date_tag="mature", prc_type="close")
        sig_df["ret"] = sig_df["close_mature"] / sig_df["close_exe"] - 1
        ret_theory = sig_df["value"] @ sig_df["ret"]
        sig_df["pnl"] = (sig_df["close_mature"] - sig_df["close_exe"]) * sig_df["quantity"] * sig_df["contractMultiplier"] * sig_df["direction"]
        ret_actual = (pnl_sum := sig_df["pnl"].sum()) / available_amount
        pnl_df = pd.DataFrame({"sid": [sig_id],
                               "openDate": [open_date], "closeDate": [close_date],
                               "retTheory": [ret_theory * 100], "pnl": [f"{pnl_sum:.2f}"], "retActual": [ret_actual * 100]})
        self.__save_pnl_df(pnl_df, sig_id=sig_id, open_date=prev_exe_date, close_date=exe_date)
        print(pnl_df)
        return 0


def cal_positions_and_trades_mp(
        proc_num: int,
        sids: list[str], exe_date: str,
        init_premium: float,
        instruments_universe: list[str],
        signals_opt_dir: str,
        md_by_instru_dir: str,
        major_minor_dir: str,
        simu_positions_and_trades_dir: str,
        calendar_path: str,
        instru_info_tab_path: str,
        database_structure: dict[str, CLib1Tab1],
):
    t0 = dt.datetime.now()

    cpt = CCalculatorPosAndTrades(
        instruments_universe=instruments_universe,
        md_by_instru_dir=md_by_instru_dir,
        major_minor_dir=major_minor_dir,
        simu_positions_and_trades_dir=simu_positions_and_trades_dir,
        calendar_path=calendar_path,
        instru_info_tab_path=instru_info_tab_path,
    )

    pool = mp.Pool(processes=proc_num)
    for sid in sids:
        pool.apply_async(cpt.cal_simu_positions, args=(sid, exe_date, init_premium, signals_opt_dir, database_structure))
    pool.close()
    pool.join()

    pool = mp.Pool(processes=proc_num)
    for sid in sids:
        pool.apply_async(cpt.cal_simu_trades, args=(sid, exe_date))
    pool.close()
    pool.join()

    pool = mp.Pool(processes=proc_num)
    for sid in sids:
        pool.apply_async(cpt.cal_simu_pnl, args=(sid, exe_date, init_premium))
    pool.close()
    pool.join()

    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
