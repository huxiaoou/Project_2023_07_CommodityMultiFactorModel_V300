import os
import datetime as dt
import itertools as ittl
import multiprocessing as mp
import pandas as pd
from skyrim.riften import CNAV
from skyrim.whiterun import SetFontGreen
from skyrim.winterhold import plot_lines


class CEvaluation(object):
    def __init__(self, eval_id: str, simu_ids: list[str],
                 indicators: list[str],
                 simu_save_dir: str, eval_save_dir: str, annual_risk_free_rate: float):
        self.simu_ids = simu_ids
        self.indicators = indicators
        self.simu_save_dir = simu_save_dir
        self.eval_save_dir = eval_save_dir
        self.annual_risk_free_rate = annual_risk_free_rate
        self.eval_id = eval_id

    def __get_performance_evaluation(self, simu_id: str) -> dict:
        simu_nav_file = f"nav-{simu_id}.csv.gz"
        simu_nav_path = os.path.join(self.simu_save_dir, simu_nav_file)
        simu_nav_df = pd.read_csv(simu_nav_path, dtype={"trade_date": str}).set_index("trade_date")
        nav = CNAV(t_raw_nav_srs=simu_nav_df["netRet"], t_annual_rf_rate=self.annual_risk_free_rate, t_type="RET")
        nav.cal_all_indicators(t_method="linear")
        return nav.to_dict(t_type="eng")

    def _add_args_to(self, res: dict, simu_id: str):
        # res.update(kwargs)
        pass

    def _set_index(self, eval_df: pd.DataFrame):
        # eval_df.set_index(indexes:list[str], inplace=True)
        pass

    def __save(self, res_data: list[dict], printout: bool = False):
        eval_df = pd.DataFrame(res_data)
        self._set_index(eval_df)
        eval_file = f"eval-{self.eval_id}.csv"
        eval_path = os.path.join(self.eval_save_dir, eval_file)
        eval_df[self.indicators].to_csv(eval_path, float_format="%.4f")
        if printout:
            print(eval_df[self.indicators])
        return 0

    def main(self, printout: bool = False):
        res_data = []
        for simu_id in self.simu_ids:
            res = self.__get_performance_evaluation(simu_id)
            self._add_args_to(res, simu_id)
            res_data.append(res)
        self.__save(res_data, printout)
        print(f"... @ {SetFontGreen(f'{dt.datetime.now()}')} nav evaluation for {SetFontGreen(self.eval_id)} calculated")
        return 0


class CEvaluationWithFactorClass(CEvaluation):
    def __init__(self, factors_classification: dict[str, str], **kwargs):
        super().__init__(**kwargs)
        self.factors_classification = factors_classification

    def _add_args_to(self, res: dict, simu_id: str):
        factor = simu_id.split("_")[0]
        factor_class = self.factors_classification[factor]
        res.update({"factor": factor, "factor_class": factor_class})
        return 0

    def _set_index(self, eval_df: pd.DataFrame):
        eval_df.set_index(["factor", "factor_class"], inplace=True)
        return 0


class CEvaluationPortfolio(CEvaluation):
    def _add_args_to(self, res: dict, simu_id: str):
        res.update({"portfolio": simu_id})
        return 0

    def _set_index(self, eval_df: pd.DataFrame):
        eval_df.set_index(["portfolio"], inplace=True)
        return 0


def eval_hedge_mp(proc_num: int, factors: list[str], factors_neutral: list[str], uni_props: tuple[float], **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for (fs, fid), uni_prop in ittl.product([(factors, "raw"), (factors_neutral, "neu")], uni_props):
        uni_prop_lbl = f"UHP{int(uni_prop * 10):02d}"
        simu_ids = [f"{_}_{uni_prop_lbl}" for _ in fs]
        eval_id = f"factors_{fid}_{uni_prop_lbl}"
        agent_eval = CEvaluationWithFactorClass(eval_id=eval_id, simu_ids=simu_ids, **kwargs)
        pool.apply_async(agent_eval.main)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print(f"... {SetFontGreen('Summary for Hedge Test')} calculated")
    print(f"... total time consuming:{SetFontGreen(f'{(t1 - t0).total_seconds():.2f}')} seconds")
    return 0


def concat_eval_results(uni_props: tuple[float], eval_save_dir: str):
    raw_dfs, neu_dfs = [], []
    for uni_prop in uni_props:
        uni_prop_lbl = f"UHP{int(uni_prop * 10):02d}"
        sub_eval_raw_file = f"eval-factors_raw_{uni_prop_lbl}.csv"
        sub_eval_neu_file = f"eval-factors_neu_{uni_prop_lbl}.csv"
        sub_eval_raw_path = os.path.join(eval_save_dir, sub_eval_raw_file)
        sub_eval_neu_path = os.path.join(eval_save_dir, sub_eval_neu_file)
        sub_eval_raw_df = pd.read_csv(sub_eval_raw_path)
        sub_eval_neu_df = pd.read_csv(sub_eval_neu_path)
        sub_eval_raw_df["uni_prop"] = uni_prop
        sub_eval_neu_df["uni_prop"] = uni_prop
        raw_dfs.append(sub_eval_raw_df)
        neu_dfs.append(sub_eval_neu_df)

    raw_df_concat = pd.concat(raw_dfs, axis=0, ignore_index=True).sort_values(by=["factor", "sharpe_ratio"], ascending=[True, False])
    neu_df_concat = pd.concat(neu_dfs, axis=0, ignore_index=True).sort_values(by=["factor", "sharpe_ratio"], ascending=[True, False])
    raw_concat_file = os.path.join(eval_save_dir, "eval-factors_raw-concat.csv")
    neu_concat_file = os.path.join(eval_save_dir, "eval-factors_neu-concat.csv")
    raw_df_concat.to_csv(raw_concat_file, index=False, float_format="%.2f")
    neu_df_concat.to_csv(neu_concat_file, index=False, float_format="%.2f")
    return 0


def plot_selected_factors_and_uni_prop(
        selected_factors_and_uni_prop: tuple[tuple], save_id: str,
        simu_save_dir: str, eval_save_dir: str):
    nav_data = {}
    for factor, uni_prop in selected_factors_and_uni_prop:
        uni_prop_lbl = f"UHP{int(uni_prop * 10):02d}"
        if save_id == "raw":
            simu_id = f"{factor}_{uni_prop_lbl}"
        else:
            simu_id = f"{factor}_WS_{uni_prop_lbl}"
        simu_nav_file = f"nav-{simu_id}.csv.gz"
        simu_nav_path = os.path.join(simu_save_dir, simu_nav_file)
        simu_nav_df = pd.read_csv(simu_nav_path, dtype={"trade_date": str}).set_index("trade_date")
        nav_data[simu_id] = simu_nav_df["nav"]
    nav_df = pd.DataFrame(nav_data)
    plot_lines(t_plot_df=nav_df, t_fig_name=f"selected-factors_and_uni_prop-{save_id}-nav", t_save_dir=eval_save_dir)
    print(f"... @ {dt.datetime.now()} selected factors and uni-prop for {SetFontGreen(save_id)} plotted")
    return 0
