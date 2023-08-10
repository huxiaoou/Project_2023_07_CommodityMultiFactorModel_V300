import os
import pandas as pd
from skyrim.whiterun import CCalendar, SetFontGreen
from skyrim.falkreath import CLib1Tab1, CTable, CManagerLibReader, CManagerLibWriter
from factors.factors_shared import get_factor_exposure_lib_struct


def get_signal_lib_struct(signal: str) -> CLib1Tab1:
    return CLib1Tab1(
        t_lib_name=f"{signal}.db",
        t_tab=CTable({
            "table_name": signal,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    )


class CSignal(object):
    def __init__(self, sig_id: str, sig_save_dir: str, calendar: CCalendar, **kwargs):
        self.sig_id = sig_id
        self.sig_save_dir = sig_save_dir
        self._set_id()
        self.sig_lib_struct = get_signal_lib_struct(sig_id)
        self.calendar = calendar

    def _set_id(self):
        pass

    def __get_lib_reader(self) -> CManagerLibReader:
        sig_lib_reader = CManagerLibReader(self.sig_save_dir, self.sig_lib_struct.m_lib_name)
        sig_lib_reader.set_default(self.sig_lib_struct.m_tab.m_table_name)
        return sig_lib_reader

    def __get_lib_writer(self, run_mode: str) -> CManagerLibWriter:
        sig_lib_writer = CManagerLibWriter(self.sig_save_dir, self.sig_lib_struct.m_lib_name)
        sig_lib_writer.initialize_table(self.sig_lib_struct.m_tab, run_mode in ["O"])
        return sig_lib_writer

    def __check_continuity(self, run_mode: str, bgn_date: str) -> int:
        if run_mode in ["A"]:
            reader = self.__get_lib_reader()
            res = reader.check_continuity(bgn_date, self.calendar)
            reader.close()
            return res
        else:
            return 0

    def _get_update_df(self, run_mode: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        pass

    def save(self, update_df: pd.DataFrame, run_mode: str, use_index: bool):
        sig_lib_writer = self.__get_lib_writer(run_mode)
        sig_lib_writer.update(update_df, use_index)
        sig_lib_writer.close()
        return 0

    def main(self, run_mode: str, bgn_date: str, stp_date: str):
        if self.__check_continuity(run_mode, bgn_date) == 0:
            update_df = self._get_update_df(run_mode, bgn_date, stp_date)

        return 0


class CSignalFromSrcFactor(CSignal):
    def __init__(self, src_factor_id: str, src_factor_dir: str, **kwargs):
        self.src_factor_id = src_factor_id
        self.src_factor_dir = src_factor_dir
        self.src_factor_lib_struct = get_factor_exposure_lib_struct(src_factor_id)
        super().__init__(**kwargs)

    def __get_src_factor_lib_reader(self):
        src_factor_lib_reader = CManagerLibReader(self.src_factor_dir, self.src_factor_lib_struct.m_lib_name)
        src_factor_lib_reader.set_default(self.src_factor_lib_struct.m_tab.m_table_name)
        return 0


class CSignalHedge(CSignalFromSrcFactor):
    def __init__(self, prop: float, **kwargs):
        self.prop = prop
        super().__init__(**kwargs)

    def _set_id(self):
        self.sig_id = self.sig_id + f"{int(self.prop * 10):02d}"
