import pandas as pd
from skyrim.whiterun import CCalendar, SetFontRed, SetFontGreen
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriter


class CFactors(object):
    def __init__(self,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendar,
                 ):
        self.m_universe = concerned_instruments_universe
        self.m_factors_exposure_dir = factors_exposure_dir
        self.m_database_structure = database_structure
        self.m_calendar = calendar
        self.m_factor: str = "Factor Not Init"

    def __get_instrument_factor_exposure(self, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        pass

    def __get_dst_lib_reader(self):
        factor_lib_struct = self.m_database_structure[self.m_factor]
        factor_lib = CManagerLibReader(
            t_db_name=factor_lib_struct.m_lib_name,
            t_db_save_dir=self.m_factors_exposure_dir
        )
        factor_lib.set_default(t_default_table_name=factor_lib_struct.m_tab.m_table_name)
        return factor_lib

    def __get_dst_lib_writer(self, run_mode: str):
        factor_lib_struct = self.m_database_structure[self.m_factor]
        factor_lib = CManagerLibWriter(
            t_db_name=factor_lib_struct.m_lib_name,
            t_db_save_dir=self.m_factors_exposure_dir
        )
        factor_lib.initialize_table(t_table=factor_lib_struct.m_tab, t_remove_existence=run_mode in ["O"])
        return factor_lib

    def __check_continuity(self, run_mode: str, bgn_date: str):
        factor_lib = self.__get_dst_lib_reader()
        dst_lib_is_continuous = factor_lib.check_continuity(
            append_date=bgn_date, t_calendar=self.m_calendar) if run_mode in ["A"] else 0
        factor_lib.close()
        return dst_lib_is_continuous

    def __get_update_df(self, run_mode: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        all_factor_data = {}
        for instrument in self.m_universe:
            all_factor_data[instrument] = self.__get_instrument_factor_exposure(run_mode, bgn_date, stp_date)
        all_factor_df = pd.DataFrame(all_factor_data)
        update_df = all_factor_df.stack().reset_index(level=1)
        return update_df

    def __save(self, update_df: pd.DataFrame, using_index: bool, run_mode: str):
        factor_lib = self.__get_dst_lib_writer(run_mode)
        factor_lib.update(t_update_df=update_df, t_using_index=using_index)
        factor_lib.close()
        return 0

    def core(self, run_mode: str, bgn_date: str, stp_date: str):
        if self.__check_continuity(run_mode, bgn_date) == 0:
            update_df = self.__get_update_df(run_mode, bgn_date, stp_date)
            self.__save(update_df, using_index=True, run_mode=run_mode)
        else:
            print(f"... {SetFontGreen(self.m_factor)} {SetFontRed('FAILED')} to update")

