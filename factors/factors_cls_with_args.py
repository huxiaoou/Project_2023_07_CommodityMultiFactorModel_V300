import numpy as np
import pandas as pd
from factors.factors_cls_base import (CFactorsWithMajorReturnAndArgWin, CFactorsWithMajorReturnAndMarketReturn,
                                      CFactorsWithMajorReturnAndExchangeRate, CFactorsWithMajorReturnAndMacroEconomic)


def cal_rolling_corr(df: pd.DataFrame, x: str, y: str, rolling_window: int) -> pd.Series:
    df["xy"] = (df[x] * df[y]).rolling(window=rolling_window).mean()
    df["xx"] = (df[x] * df[x]).rolling(window=rolling_window).mean()
    df["yy"] = (df[y] * df[y]).rolling(window=rolling_window).mean()
    df["x"] = df[x].rolling(window=rolling_window).mean()
    df["y"] = df[y].rolling(window=rolling_window).mean()

    df["cov_xy"] = df["xy"] - df["x"] * df["y"]
    df["cov_xx"] = df["xx"] - df["x"] * df["x"]
    df["cov_yy"] = df["yy"] - df["y"] * df["y"]

    df.loc[np.abs(df["cov_xx"]) <= 1e-10, "cov_xx"] = 0
    df.loc[np.abs(df["cov_yy"]) <= 1e-10, "cov_yy"] = 0

    df["sqrt_cov_xx_yy"] = np.sqrt(df["cov_xx"] * df["cov_yy"]).fillna(0)
    s = df[["cov_xy", "sqrt_cov_xx_yy"]].apply(
        lambda z: 0 if z["sqrt_cov_xx_yy"] == 0 else z["cov_xy"] / z["sqrt_cov_xx_yy"], axis=1)
    return s


def cal_rolling_beta(df: pd.DataFrame, x: str, y: str, rolling_window: int) -> pd.Series:
    df["xy"] = (df[x] * df[y]).rolling(window=rolling_window).mean()
    df["xx"] = (df[x] * df[x]).rolling(window=rolling_window).mean()
    df["x"] = df[x].rolling(window=rolling_window).mean()
    df["y"] = df[y].rolling(window=rolling_window).mean()
    df["cov_xy"] = df["xy"] - df["x"] * df["y"]
    df["cov_xx"] = df["xx"] - df["x"] * df["x"]
    s = df["cov_xy"] / df["cov_xx"]
    return s


class CFactorSKEW(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"SKEW{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
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
        self.factor_id = f"VOL{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
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
        self.factor_id = f"RVOL{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
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
        db_reader = self.manager_major_return.get_db_reader()
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


class CFactorsCTP(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"CTP{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "volume", "oi", "instru_idx"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)

        df["aver_oi"] = df["oi"].rolling(window=2).mean()
        df["turnover"] = df["volume"] / df["aver_oi"]
        x, y = "turnover", "instru_idx"
        s = -cal_rolling_corr(df=df, x=x, y=y, rolling_window=self.arg_win)
        return self._truncate(s, bgn_date)


class CFactorsCVP(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"CVP{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "volume", "instru_idx"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)

        x, y = "volume", "instru_idx"
        s = -cal_rolling_corr(df=df, x=x, y=y, rolling_window=self.arg_win)
        return self._truncate(s, bgn_date)


class CFactorsCSP(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"CSP{self.arg_win:03d}"
        return 0

    def _set_base_date(self, bgn_date: str, stp_date: str):
        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        self.base_date = self.calendar.get_next_date(iter_dates[0], -self.arg_win - self._near_short_term + 1)
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return", "instru_idx"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)

        df["sigma"] = df["major_return"].rolling(window=self._near_short_term).std()
        x, y = "sigma", "instru_idx"
        s = -cal_rolling_corr(df=df, x=x, y=y, rolling_window=self.arg_win)
        return self._truncate(s, bgn_date)


class CFactorsVAL(CFactorsWithMajorReturnAndArgWin):
    def _set_factor_id(self):
        self.factor_id = f"VAL{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        __now_aver_days = 21
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "close"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)

        df["near"] = df["close"].rolling(window=self._near_short_term).mean()
        df["hist"] = df["close"].rolling(window=self.arg_win).mean()
        s = df["near"] / df["hist"] - 1
        return self._truncate(s, bgn_date)


class CFactorBeta(CFactorsWithMajorReturnAndMarketReturn):
    def _set_factor_id(self):
        self.factor_id = f"BETA{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        __now_aver_days = 21
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)

        df["market"] = self.manager_market_return.df["market"]
        s = cal_rolling_beta(df, x="market", y="major_return", rolling_window=self.arg_win)
        return self._truncate(s, bgn_date)


class CFactorCBeta(CFactorsWithMajorReturnAndExchangeRate):
    def _set_factor_id(self):
        self.factor_id = f"CBETA{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        __now_aver_days = 21
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)

        df["exchange"] = self.manager_exchange_rate.df["pct_chg"] / 100
        s = cal_rolling_beta(df, x="exchange", y="major_return", rolling_window=self.arg_win)
        return self._truncate(s, bgn_date)


class CFactorIBeta(CFactorsWithMajorReturnAndMacroEconomic):
    def _set_factor_id(self):
        self.factor_id = f"IBETA{self.arg_win:03d}"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        __now_aver_days = 21
        self._set_base_date(bgn_date, stp_date)
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)

        df["cpi"] = self.manager_macro_economic.df["cpi_rate"]
        s = cal_rolling_beta(df, x="exchange", y="major_return", rolling_window=self.arg_win)
        return self._truncate(s, bgn_date)
