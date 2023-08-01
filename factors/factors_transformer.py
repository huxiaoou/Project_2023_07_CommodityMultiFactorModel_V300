import datetime as dt
import pandas as pd
import multiprocessing as mp
from skyrim.whiterun import CCalendar, SetFontGreen
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from factors.factors_base import CFactors


# -----------------------------------------
# ----- Part III: CFactors Derivative -----
# -----------------------------------------

class CFactorsTransformer(CFactors):
    def __init__(self, src_factor_id: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendar,
                 ):
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.src_factor_id = src_factor_id
        self.base_date = ""

    def _set_base_date(self, bgn_date: str, stp_date: str):
        pass

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

    def _transform(self, pivot_table: pd.DataFrame) -> pd.DataFrame:
        pass

    @staticmethod
    def __truncate(new_df: pd.DataFrame, bgn_date: str) -> pd.DataFrame:
        filter_dates = new_df.index >= bgn_date
        return new_df.loc[filter_dates]

    def _get_update_df(self, run_mode: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        self._set_base_date(bgn_date, stp_date)
        src_df = self.__load_src_factor_data(stp_date)
        pivot_df = pd.pivot_table(data=src_df, index="trade_date", columns="instrument", values="value")
        new_df = self._transform(pivot_df)
        self.__truncate(new_df, bgn_date)
        update_df = new_df.stack().reset_index(level=1)
        return update_df


class CFactorsTransformerSum(CFactorsTransformer):
    def __init__(self, sum_win: int, src_factor_id: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendar,
                 ):
        super().__init__(src_factor_id, concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.sum_win = sum_win

    def _set_factor_id(self):
        self.factor_id = f"{self.src_factor_id}S{self.sum_win:03d}"
        return 0

    def _set_base_date(self, bgn_date: str, stp_date: str):
        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        self.base_date = self.calendar.get_next_date(iter_dates[0], -self.sum_win + 1)
        return 0

    def _transform(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pivot_df.rolling(window=self.sum_win).sum()
        return new_df


def mp_cal_factor_transform_sum(proc_num: int,
                                sum_wins: tuple[int], src_factor_id: str,
                                run_mode, bgn_date, stp_date,
                                **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for sum_win in sum_wins:
        transformer = CFactorsTransformerSum(sum_win, src_factor_id, **kwargs)
        pool.apply_async(transformer.core, args=(run_mode, bgn_date, stp_date))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print(f"... {SetFontGreen('sum')} of {SetFontGreen(src_factor_id)} transformed")
    print(f"... total time consuming: {SetFontGreen(f'{(t1 - t0).total_seconds():.2f}')} seconds")
