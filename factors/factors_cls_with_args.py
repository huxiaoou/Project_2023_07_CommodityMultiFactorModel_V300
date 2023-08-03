import numpy as np
import pandas as pd
from factors.factors_cls_base import (CFactorsWithMajorReturnAndArgWin, CFactorsWithMajorReturnAndMarketReturn,
                                      CFactorsWithMajorReturnAndExchangeRate, CFactorsWithMajorReturnAndMacroEconomic)


class CFactorSKEW(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"SKEW{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.m_manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)
        s = df["major_return"].rolling(window=self.arg_win).skew()
        return self._truncate(s, bgn_date)


class CFactorsVOL(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"VOl{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.m_manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)
        s = df["major_return"].rolling(window=self.arg_win).std() * np.sqrt(252)
        return self._truncate(s, bgn_date)


class CFactorsRVOL(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"RVOl{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.m_manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "open", "high", "low", "close"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)
        rho = df["high"] / df["open"] - 1
        rhc = df["high"] / df["close"] - 1
        rlo = df["low"] / df["open"] - 1
        rlc = df["low"] / df["close"] - 1
        s2 = (rho * rhc / 2 + rlo * rlc / 2).rolling(window=self.arg_win).mean()
        s = np.sqrt(252 * s2)
        return self._truncate(s, bgn_date)


class CFactorsCV(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"CV{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.m_manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)
        sd = df["major_return"].rolling(window=self.arg_win).std()
        mu = df["major_return"].rolling(window=self.arg_win).mean()
        s = sd / mu.abs() / np.sqrt(252)
        return self._truncate(s, bgn_date)
