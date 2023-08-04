import os
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendarMonthly, SetFontRed, SetFontGreen
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriter


# -----------------------------------------
# ---------- Part I: Data Reader-----------
# -----------------------------------------
class CReaderMarketReturn(object):
    def __init__(self, market_return_dir: str, market_return_file: str):
        src_path = os.path.join(market_return_dir, market_return_file)
        self.df = pd.read_csv(src_path, dtype={"trade_date": str}).set_index("trade_date")


class CReaderExchangeRate(object):
    def __init__(self, forex_dir: str, exchange_rate_file: str):
        src_path = os.path.join(forex_dir, exchange_rate_file)
        self.df = pd.read_excel(src_path)
        self.df["trade_date"] = self.df["Date"].strftime("%Y%m%d")
        self.df.drop(labels=["Date"], axis=1, inplace=True)
        self.df.set_index("trade_date", inplace=True)


class CReaderMacroEconomic(object):
    def __init__(self, macro_economic_dir: str, cpi_m2_file: str):
        src_path = os.path.join(macro_economic_dir, cpi_m2_file)
        self.df = pd.read_excel(src_path)
        self.df["trade_month"] = self.df["trade_month"].map(lambda _: _.strftime("%Y%m"))
        self.df.set_index("trade_month", inplace=True)


class CDbByInstrument(object):
    # applied to:
    # instrument_member.db
    # instrument_volume.db
    # major_minor.db
    # major_return.db
    def __init__(self, db_save_dir: str, db_name: str):
        self.db_save_dir = db_save_dir
        self.db_name = db_name

    def get_db_reader(self) -> CManagerLibReader:
        db_reader = CManagerLibReader(self.db_save_dir, self.db_name)
        return db_reader


class CCSVByInstrument(object):
    def __init__(self, instruments: list[str], csvs_save_dir: str, src_data_type: str):
        self.instruments = instruments
        self.csvs_save_dir = csvs_save_dir
        csv_file_format = {
            "BASIS": "{}.BASIS.csv.gz",
            "STOCK": "{}.STOCK.csv.gz",
            "MDC": "{}.md.close.csv.gz",
            "MDO": "{}.md.open.csv.gz",
            "MDS": "{}.md.settle.csv.gz",
        }.get(src_data_type.upper(), None)

        self.manger_dfs = {}
        if csv_file_format:
            for instrument in instruments:
                src_file = csv_file_format.format(instrument)
                src_path = os.path.join(self.csvs_save_dir, src_file)
                self.manger_dfs[instrument] = pd.read_csv(src_path, dtype={"trade_date": str}).set_index("trade_date")


class CMdByInstrument(CCSVByInstrument):
    # applied to: md_by_instru
    def get_price_for_contract_at_day(self, instrument: str, contract: str, trade_date: str) -> float:
        return self.manger_dfs[instrument].at[trade_date, contract]


class CFundByInstrument(CCSVByInstrument):
    # applied to: fundamental_by_instru
    def get_var_by_instrument(self, instrument: str, var_name: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        df = self.manger_dfs[instrument][[var_name]]
        filter_dates = (df.index >= bgn_date) & (df.index < stp_date)
        return df.loc[filter_dates]


# -----------------------------------------
# ----------- Part II: CFactors -----------
# -----------------------------------------

class CFactors(object):
    def __init__(self,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly,
                 ):
        self.universe = concerned_instruments_universe
        self.factors_exposure_dir = factors_exposure_dir
        self.database_structure = database_structure
        self.calendar = calendar
        self.factor_id: str = "FactorIdNotInit"

    def __get_dst_lib_reader(self) -> CManagerLibReader:
        factor_lib_struct = self.database_structure[self.factor_id]
        factor_lib = CManagerLibReader(
            t_db_name=factor_lib_struct.m_lib_name,
            t_db_save_dir=self.factors_exposure_dir
        )
        factor_lib.set_default(t_default_table_name=factor_lib_struct.m_tab.m_table_name)
        return factor_lib

    def __get_dst_lib_writer(self, run_mode: str) -> CManagerLibWriter:
        factor_lib_struct = self.database_structure[self.factor_id]
        factor_lib = CManagerLibWriter(
            t_db_name=factor_lib_struct.m_lib_name,
            t_db_save_dir=self.factors_exposure_dir
        )
        factor_lib.initialize_table(t_table=factor_lib_struct.m_tab, t_remove_existence=run_mode in ["O"])
        return factor_lib

    def __check_continuity(self, run_mode: str, bgn_date: str):
        factor_lib = self.__get_dst_lib_reader()
        dst_lib_is_continuous = factor_lib.check_continuity(
            append_date=bgn_date, t_calendar=self.calendar) if run_mode in ["A"] else 0
        factor_lib.close()
        return dst_lib_is_continuous

    def __save(self, update_df: pd.DataFrame, using_index: bool, run_mode: str):
        factor_lib = self.__get_dst_lib_writer(run_mode)
        factor_lib.update(t_update_df=update_df, t_using_index=using_index)
        factor_lib.close()
        return 0

    def _set_factor_id(self):
        pass

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        pass

    def _get_update_df(self, run_mode: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        all_factor_data = {}
        for instrument in self.universe:
            all_factor_data[instrument] = self._get_instrument_factor_exposure(instrument, run_mode, bgn_date, stp_date)
        all_factor_df = pd.DataFrame(all_factor_data)
        update_df = all_factor_df.stack().reset_index(level=1)
        return update_df

    def core(self, run_mode: str, bgn_date: str, stp_date: str):
        self._set_factor_id()
        if self.__check_continuity(run_mode, bgn_date) == 0:
            update_df = self._get_update_df(run_mode, bgn_date, stp_date)
            self.__save(update_df, using_index=True, run_mode=run_mode)
        else:
            print(f"... {SetFontGreen(self.factor_id)} {SetFontRed('FAILED')} to update")
        return 0

    @staticmethod
    def truncate_series(new_srs: pd.Series, bgn_date: str) -> pd.Series:
        filter_dates = new_srs.index >= bgn_date
        return new_srs.loc[filter_dates]

    @staticmethod
    def truncate_dataFrame(new_df: pd.DataFrame, bgn_date: str) -> pd.DataFrame:
        filter_dates = new_df.index >= bgn_date
        return new_df.loc[filter_dates]


class CFactorsWithMajorReturn(CFactors):
    def __init__(self, futures_by_instrument_dir: str, major_return_db_name: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_major_return = CDbByInstrument(futures_by_instrument_dir, major_return_db_name)


class CFactorsWithMajorReturnAndArgWin(CFactorsWithMajorReturn):
    def __init__(self, arg_win: int, futures_by_instrument_dir: str, major_return_db_name: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(futures_by_instrument_dir, major_return_db_name, concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.arg_win = arg_win
        self._near_short_term = 21  # for CSP and VAL

    def _set_base_date(self, bgn_date: str, stp_date: str):
        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        self.base_date = self.calendar.get_next_date(iter_dates[0], -self.arg_win + 1)
        return 0


class CFactorsWithMajorReturnAndMarketReturn(CFactorsWithMajorReturnAndArgWin):
    def __init__(self, arg_win: int, futures_by_instrument_dir: str, major_return_db_name: str,
                 market_return_dir, market_return_file,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(arg_win, futures_by_instrument_dir, major_return_db_name,
                         concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_market_return = CReaderMarketReturn(market_return_dir, market_return_file)


class CFactorsWithMajorReturnAndExchangeRate(CFactorsWithMajorReturnAndArgWin):
    def __init__(self, arg_win: int, futures_by_instrument_dir: str, major_return_db_name: str,
                 forex_dir: str, exchange_rate_file: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(arg_win, futures_by_instrument_dir, major_return_db_name,
                         concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_exchange_rate = CReaderExchangeRate(forex_dir, exchange_rate_file)


class CFactorsWithMajorReturnAndMacroEconomic(CFactorsWithMajorReturnAndArgWin):
    def __init__(self, arg_win: int, futures_by_instrument_dir: str, major_return_db_name: str,
                 macro_economic_dir, cpi_m2_file,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(arg_win, futures_by_instrument_dir, major_return_db_name,
                         concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_macro_economic = CReaderMacroEconomic(macro_economic_dir, cpi_m2_file)


class CFactorsWithBasis(CFactors):
    def __init__(self,
                 fundamental_by_instru_dir: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_basis = CFundByInstrument(self.universe, fundamental_by_instru_dir, "BASIS")


class CFactorsWithStock(CFactors):
    def __init__(self,
                 fundamental_by_instru_dir: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_stock = CFundByInstrument(self.universe, fundamental_by_instru_dir, "STOCK")


class CFactorsWithMajorMinorAndMdc(CFactors):
    def __init__(self,
                 futures_by_instrument_dir: str, major_minor_db_name: str,
                 md_by_instru_dir: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_major_minor = CDbByInstrument(futures_by_instrument_dir, major_minor_db_name)
        self.manager_md = CMdByInstrument(self.universe, md_by_instru_dir, "MDC")


class CFactorsWithInstruVolume(CFactors):
    def __init__(self,
                 futures_by_instrument_dir: str, instrument_volume_db_name: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_instru_volume = CDbByInstrument(futures_by_instrument_dir, instrument_volume_db_name)


class CFactorsWithInstruVolumeAndInstruMember(CFactors):
    def __init__(self,
                 futures_by_instrument_dir: str, instrument_volume_db_name: str, instrument_member_db_name: str,
                 concerned_instruments_universe: list[str],
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly, ):
        super().__init__(concerned_instruments_universe, factors_exposure_dir, database_structure, calendar)
        self.manager_instru_volume = CDbByInstrument(futures_by_instrument_dir, instrument_volume_db_name)
        self.manager_instru_member = CDbByInstrument(futures_by_instrument_dir, instrument_member_db_name)

    def _get_net_srs(self, instrument: str, bgn_date: str, stp_date: str, x: str, y: str) -> pd.Series:
        member_db_reader = self.manager_instru_member.get_db_reader()
        member_df = member_db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", x, y],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        if len(member_df) > 0:
            member_df_agg = pd.pivot_table(data=member_df, index="trade_date", values=[x, y], aggfunc=sum)
            return member_df_agg[x] - member_df_agg[y]
        else:
            return pd.Series(data=np.nan, index=self.calendar.get_iter_list(bgn_date, stp_date, True))

    @staticmethod
    def __cal_wgt_diff(df: pd.DataFrame, x: str, y: str):
        wgt_x = df[x].abs() / df[x].abs().sum()
        wgt_y = df[y].abs() / df[y].abs().sum()
        sum_x = df[x] @ wgt_x
        sum_y = df[y] @ wgt_y
        return sum_x - sum_y

    def _get_wgt_net_srs(self, instrument: str, bgn_date: str, stp_date: str, x: str, y: str):
        member_db_reader = self.manager_instru_member.get_db_reader()
        member_df = member_db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", x, y],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        if len(member_df) > 0:
            return member_df.groupby(by="trade_date").apply(self.__cal_wgt_diff, x=x, y=y)
        else:
            return pd.Series(data=np.nan, index=self.calendar.get_iter_list(bgn_date, stp_date, True))
