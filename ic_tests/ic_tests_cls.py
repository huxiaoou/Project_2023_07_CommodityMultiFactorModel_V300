import datetime as dt
import numpy as np
import pandas as pd
import multiprocessing as mp
from skyrim.whiterun import CCalendar, SetFontGreen
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriter


class CICTests(object):
    def __init__(self, factor: str, ic_tests_dir: str,
                 available_universe_dir: str, exposure_dir: str, test_return_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendar,
                 ):
        self.factor = factor
        self.available_universe_dir = available_universe_dir

        self.database_structure = database_structure
        self.calendar = calendar
        self.dst_dir = ic_tests_dir

        self.src_factor_exposure_lib_id = ""
        self.src_test_return_lib_id = ""
        self.ic_test_lib_id = ""

        self.exposure_dir = exposure_dir
        self.test_return_dir = test_return_dir

    @staticmethod
    def corr_one_day(df: pd.DataFrame, x: str, y: str, method: str):
        res = df[[x, y]].corr(method=method).at[x, y] if len(df) > 2 else 0
        return 0 if np.isnan(res) else res

    def set_id(self):
        pass

    def get_bridge_dates(self, bgn_date: str, stp_date: str):
        __test_lag = 1
        __test_window = 1
        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        base_bgn_date = self.calendar.get_next_date(iter_dates[0], -__test_lag - __test_window)
        base_stp_date = self.calendar.get_next_date(iter_dates[-1], -__test_lag - __test_window + 1)
        base_dates = self.calendar.get_iter_list(base_bgn_date, base_stp_date, True)
        bridge_dates_df = pd.DataFrame({"base_date": base_dates, "trade_date": iter_dates})
        return base_bgn_date, bridge_dates_df

    def get_available_universe(self, base_bgn_date: str, stp_date: str) -> pd.DataFrame:
        available_universe_lib_structure = self.database_structure["available_universe"]
        available_universe_lib = CManagerLibReader(t_db_name=available_universe_lib_structure.m_lib_name, t_db_save_dir=self.available_universe_dir)
        available_universe_lib.set_default(available_universe_lib_structure.m_tab.m_table_name)
        available_universe_df = available_universe_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", base_bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument"])
        available_universe_lib.close()
        return available_universe_df

    def get_factor_exposure(self, base_bgn_date: str, stp_date: str) -> pd.DataFrame:
        factor_lib_structure = self.database_structure[self.src_factor_exposure_lib_id]
        factor_lib = CManagerLibReader(t_db_name=factor_lib_structure.m_lib_name, t_db_save_dir=self.exposure_dir)
        factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)
        factor_df = factor_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", base_bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"])
        factor_lib.close()
        return factor_df

    def get_test_return(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        test_return_lib_structure = self.database_structure[self.src_test_return_lib_id]
        test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=self.test_return_dir)
        test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)
        test_return_df = test_return_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"])
        test_return_lib.close()
        return test_return_df

    def check_continuity(self, run_mode: str, bgn_date: str):
        if run_mode == "A":
            ic_test_lib_structure = self.database_structure[self.ic_test_lib_id]
            ic_test_lib = CManagerLibReader(t_db_name=ic_test_lib_structure.m_lib_name, t_db_save_dir=self.dst_dir)
            return ic_test_lib.check_continuity(bgn_date, self.calendar, False, ic_test_lib_structure.m_tab.m_table_name)
        else:
            return 0

    def save(self, update_df: pd.DataFrame, run_mode: str):
        ic_test_lib_structure = self.database_structure[self.ic_test_lib_id]
        ic_test_lib = CManagerLibWriter(t_db_name=ic_test_lib_structure.m_lib_name, t_db_save_dir=self.dst_dir)
        ic_test_lib.initialize_table(t_table=ic_test_lib_structure.m_tab, t_remove_existence=run_mode in ["O"])
        ic_test_lib.update(t_update_df=update_df, t_using_index=True)
        ic_test_lib.close()
        return 0

    def main(self, run_mode: str, bgn_date: str, stp_date: str):
        self.set_id()
        if self.check_continuity(run_mode, bgn_date) == 0:
            base_bgn_date, bridge_dates_df = self.get_bridge_dates(bgn_date, stp_date)
            available_universe_df = self.get_available_universe(base_bgn_date, stp_date)
            factor_df = self.get_factor_exposure(base_bgn_date, stp_date)
            test_return_df = self.get_test_return(bgn_date, stp_date)

            factors_exposure_df = pd.merge(left=available_universe_df, right=factor_df, on=["trade_date", "instrument"], how="inner"
                                           ).rename(mapper={"trade_date": "base_date"}, axis=1)
            test_return_df_expand = pd.merge(left=bridge_dates_df, right=test_return_df, on="trade_date", how="right")
            test_input_df = pd.merge(left=factors_exposure_df, right=test_return_df_expand, on=["base_date", "instrument"], how="inner", suffixes=("_e", "_r"))

            res_srs = test_input_df.groupby(by="trade_date", group_keys=True).apply(self.corr_one_day, x="value_e", y="value_r", method="spearman")
            update_df = pd.DataFrame({"value": res_srs})
            self.save(update_df, run_mode)
        return 0


class CICTestsRaw(CICTests):
    def set_id(self):
        self.src_factor_exposure_lib_id = self.factor
        self.src_test_return_lib_id = "test_return"
        self.ic_test_lib_id = f"ic-{self.factor}"


class CICTestsNeutral(CICTests):
    def __init__(self, neutral_method: str, **kwargs):
        super().__init__(**kwargs)
        self.neutral_method = neutral_method

    def set_id(self):
        self.src_factor_exposure_lib_id = f"{self.factor}_{self.neutral_method}"
        self.src_test_return_lib_id = f"test_return.{self.neutral_method}"
        self.ic_test_lib_id = f"ic-{self.factor}_{self.neutral_method}"


def cal_ic_tests_mp(proc_num: int, factors: list[str],
                    ic_tests_dir: str,
                    available_universe_dir: str, exposure_dir: str, test_return_dir: str,
                    database_structure: dict[str, CLib1Tab1], calendar: CCalendar,
                    neutral_method: str, **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for factor in factors:
        if neutral_method != "":
            agent_tests = CICTestsNeutral(neutral_method, factor=factor, ic_tests_dir=ic_tests_dir, available_universe_dir=available_universe_dir,
                                          exposure_dir=exposure_dir, test_return_dir=test_return_dir, database_structure=database_structure, calendar=calendar)
        else:
            agent_tests = CICTestsRaw(factor=factor, ic_tests_dir=ic_tests_dir, available_universe_dir=available_universe_dir,
                                      exposure_dir=exposure_dir, test_return_dir=test_return_dir, database_structure=database_structure, calendar=calendar)

        pool.apply_async(agent_tests.main, kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    if neutral_method != "":
        print(f"... {SetFontGreen(f'IC-test-{neutral_method}')} calculated")
    else:
        print(f"... {SetFontGreen(f'IC-test')} calculated")
    print(f"... total time consuming: {SetFontGreen(f'{(t1 - t0).total_seconds():.2f}')} seconds")
    return 0
