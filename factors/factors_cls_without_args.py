import pandas as pd
from factors.factors_cls_base import CFactorsWithMajorReturn


class CFactorMTM(CFactorsWithMajorReturn):
    def _set_factor_id(self):
        self.factor_id = "MTM"
        return 0

    def _get_instrument_factor_exposure(self, instrument: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.Series:
        db_reader = self.manager_major_return.get_db_reader()
        df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False, t_table_name=instrument.replace(".", "_"))
        db_reader.close()
        df.set_index("trade_date", inplace=True)
        return df["major_return"]
