import itertools as ittl
import datetime as dt
import numpy as np
import pandas as pd
import multiprocessing as mp
from skyrim.whiterun import CCalendarMonthly, SetFontGreen
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from factors.factors_cls_base import CFactors


# -----------------------------------------
# ----- Part III: CFactors Derivative -----
# -----------------------------------------

class CFactorsTransformer(CFactors):
    def __init__(self, src_factor_id: str, arg_win: int, direction: int,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly,
                 ):
        """

        :param src_factor_id:
        :param arg_win:
        :param direction: 1 or -1
        :param concerned_instruments_universe:
        :param factors_exposure_dir:
        :param database_structure:
        :param calendar:
        """
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.src_factor_id = src_factor_id
        self.arg_win = arg_win
        self.direction = direction
        self.base_date = ""

    def __get_src_lib_reader(self):
        factor_lib_struct = self.database_structure[self.src_factor_id]
        factor_lib = CManagerLibReader(
            t_db_name=factor_lib_struct.m_lib_name,
            t_db_save_dir=self.factors_exposure_dir
        )
        factor_lib.set_default(t_default_table_name=factor_lib_struct.m_tab.m_table_name)
        return factor_lib

    def __load_src_factor_data(self, stp_date: str):
        src_db_reader = self.__get_src_lib_reader()
        df = src_db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", self.base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"])
        src_db_reader.close()
        return df

    def _set_base_date(self, bgn_date: str, stp_date: str):
        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        self.base_date = self.calendar.get_next_date(iter_dates[0], -self.arg_win + 1)
        return 0

    def _transform(self, pivot_table: pd.DataFrame) -> pd.DataFrame:
        pass

    def _get_update_df(self, run_mode: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        self._set_base_date(bgn_date, stp_date)
        src_df = self.__load_src_factor_data(stp_date)
        pivot_df = pd.pivot_table(data=src_df, index="trade_date", columns="instrument", values="value")
        new_df = self._transform(pivot_df)
        new_df = self.truncate_dataFrame(new_df, bgn_date)
        new_df = new_df * self.direction
        update_df = new_df.stack().reset_index(level=1)
        return update_df


class CFactorsTransformerSum(CFactorsTransformer):
    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}S{self.arg_win:03d}"
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pivot_df.rolling(window=self.arg_win).sum()
        return new_df


class CFactorsTransformerAver(CFactorsTransformer):

    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}A{self.arg_win:03d}"
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pivot_df.rolling(window=self.arg_win).mean()
        return new_df


class CFactorsTransformerSharpe(CFactorsTransformer):
    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}SP{self.arg_win:03d}"
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        mean_df = pivot_df.rolling(window=self.arg_win).mean()
        std_df = pivot_df.rolling(window=self.arg_win).std()
        new_df = mean_df / std_df * np.sqrt(252)
        return new_df


class CFactorsTransformerBreakRatio(CFactorsTransformer):
    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}BR{self.arg_win:03d}"
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pivot_df / pivot_df.rolling(self.arg_win).mean() - 1
        return new_df


class CFactorsTransformerBreakDiff(CFactorsTransformer):

    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}BD{self.arg_win:03d}"
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pivot_df - pivot_df.rolling(self.arg_win).mean()
        return new_df


class CFactorsTransformerLagRatio(CFactorsTransformer):

    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}LR{self.arg_win:03d}"
        return 0

    def _set_base_date(self, bgn_date: str, stp_date: str):
        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        self.base_date = self.calendar.get_next_date(iter_dates[0], -self.arg_win)
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pivot_df / pivot_df.shift(self.arg_win) - 1
        return new_df


class CFactorsTransformerLagDiff(CFactorsTransformer):

    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}LD{self.arg_win:03d}"
        return 0

    def _set_base_date(self, bgn_date: str, stp_date: str):
        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        self.base_date = self.calendar.get_next_date(iter_dates[0], -self.arg_win)
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pivot_df - pivot_df.shift(self.arg_win)
        return new_df


class CMpTransformer(object):
    def __init__(self, proc_num: int,
                 src_factor_ids: list[str], transform_type: str, arg_wins: tuple[int], direction: int,
                 run_mode: str, bgn_date: str, stp_date: str, tag: str):
        self.proc_num = proc_num
        self.src_factor_ids = src_factor_ids
        self.transform_type = transform_type.upper()
        self.arg_wins = arg_wins
        self.direction = direction
        self.run_mode = run_mode
        self.bgn_date = bgn_date
        self.stp_date = stp_date
        self.tag = tag

    def mp_cal_transform(self, **kwargs):
        t0 = dt.datetime.now()
        pool = mp.Pool(processes=self.proc_num)
        for src_factor_id, arg_win in ittl.product(self.src_factor_ids, self.arg_wins):
            if self.transform_type == "SUM":
                transformer = CFactorsTransformerSum(src_factor_id, arg_win, self.direction, **kwargs)
            elif self.transform_type == "AVER":
                transformer = CFactorsTransformerAver(src_factor_id, arg_win, self.direction, **kwargs)
            elif self.transform_type == "SHARPE":
                transformer = CFactorsTransformerSharpe(src_factor_id, arg_win, self.direction, **kwargs)
            elif self.transform_type == "BD":
                transformer = CFactorsTransformerBreakDiff(src_factor_id, arg_win, self.direction, **kwargs)
            elif self.transform_type == "BR":
                transformer = CFactorsTransformerBreakRatio(src_factor_id, arg_win, self.direction, **kwargs)
            elif self.transform_type == "LD":
                transformer = CFactorsTransformerLagDiff(src_factor_id, arg_win, self.direction, **kwargs)
            elif self.transform_type == "LR":
                transformer = CFactorsTransformerLagRatio(src_factor_id, arg_win, self.direction, **kwargs)
            else:
                transformer = None
            if transformer is not None:
                pool.apply_async(transformer.core, args=(self.run_mode, self.bgn_date, self.stp_date))
        pool.close()
        pool.join()
        t1 = dt.datetime.now()
        print(f"... transformation:{SetFontGreen(self.transform_type)} of {SetFontGreen(self.tag)} accomplished")
        print(f"... total time consuming: {SetFontGreen(f'{(t1 - t0).total_seconds():.2f}')} seconds")
        return 0
