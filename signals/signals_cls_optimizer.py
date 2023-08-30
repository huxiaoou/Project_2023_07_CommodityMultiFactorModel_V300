import os
# import warnings
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendarMonthly
from skyrim.falkreath import CManagerLibReader, CManagerLibWriter
from scipy.optimize import minimize, NonlinearConstraint, LinearConstraint
from struct_lib.portfolios import get_signal_optimized_lib_struct


def portfolio_return(w: np.ndarray, mu: np.ndarray) -> float:
    return w.dot(mu)


def portfolio_variance(w: np.ndarray, sigma: np.ndarray) -> float:
    return w.dot(sigma).dot(w)


def portfolio_utility(w: np.ndarray, mu: np.ndarray, sigma: np.ndarray, lbd: float) -> float:
    # u = -2 w*m/l +wSw  <=> v = w*m - l/2 * wSw
    return -2 * portfolio_return(w, mu) / lbd + portfolio_variance(w, sigma)


def minimize_utility(mu: np.ndarray, sigma: np.ndarray, lbd) -> (np.ndarray, float):
    _p, _ = sigma.shape
    _u = np.ones(_p)
    _sigma_inv = np.linalg.inv(sigma)
    _m = _sigma_inv.dot(mu)
    _h = _sigma_inv.dot(_u)
    _d = 2 * (1 - 1 / lbd * _u.dot(_m)) / (_u.dot(_h))
    _w = 1 / lbd * _m + _d / 2 * _h
    _uty = portfolio_utility(_w, mu, sigma, lbd)
    return _w, _uty


def minimize_utility_con(mu: np.ndarray, sigma: np.ndarray, lbd: float,
                         bounds: tuple = (0, 1), pos_lim: tuple = (0, 1),
                         maxiter: int = 10000,
                         tol: float = None) -> (np.ndarray, float):
    _p, _ = sigma.shape
    # warnings.filterwarnings("ignore")
    _res = minimize(
        fun=portfolio_utility,
        x0=np.ones(_p) / _p,
        args=(mu, sigma, lbd),
        bounds=[bounds] * _p,
        # constraints=NonlinearConstraint(lambda z: np.sum(np.abs(z)), pos_lim[0], pos_lim[1]),
        constraints=LinearConstraint(np.ones(_p), pos_lim[0], pos_lim[1]),
        options={"maxiter": maxiter},
        tol=tol,
    )
    # warnings.filterwarnings("always")
    if _res.success:
        return _res.x, _res.fun
    else:
        print("ERROR! Optimizer exits with a failure")
        print("Detailed Description: {}".format(_res.message))
        return None, None


class CSignalOptimizer(object):
    def __init__(self, save_id: str, src_signal_ids: list,
                 trn_win: int, min_model_days: int, lbd: float,
                 simu_test_dir: str, optimized_dir: str,
                 calendar: CCalendarMonthly):
        self.save_id = save_id
        self.src_signal_ids, self.src_signal_qty = src_signal_ids, len(src_signal_ids)
        self.trn_win, self.lbd = trn_win, lbd
        self.min_model_days = min_model_days

        self.simu_test_dir = simu_test_dir
        self.optimized_dir = optimized_dir
        self.calendar = calendar

        self.signal_simu_ret_df = pd.DataFrame()
        self.default_weights = pd.Series(data=1 / self.src_signal_qty, index=self.src_signal_ids)
        self.optimized_struct = get_signal_optimized_lib_struct(save_id)

    def __get_optimized_lib_reader(self) -> CManagerLibReader:
        lib_reader = CManagerLibReader(self.optimized_dir, self.optimized_struct.m_lib_name)
        lib_reader.set_default(self.optimized_struct.m_tab.m_table_name)
        return lib_reader

    def __get_optimized_lib_writer(self, run_mode: str) -> CManagerLibWriter:
        lib_writer = CManagerLibWriter(self.optimized_dir, self.optimized_struct.m_lib_name)
        lib_writer.initialize_table(self.optimized_struct.m_tab, run_mode in ["O"])
        return lib_writer

    def __check_continuity(self, run_mode: str, append_month: str):
        if run_mode in ["A"]:
            lib_reader = self.__get_optimized_lib_reader()
            res = lib_reader.check_continuity_monthly(append_month, self.calendar)
            lib_reader.close()
            return res
        else:
            return 0

    def __load_simu_tests(self):
        ret_data = {}
        for src_signal_id in self.src_signal_ids:
            simu_file = f"nav-{src_signal_id}.csv.gz"
            simu_path = os.path.join(self.simu_test_dir, simu_file)
            simu_df = pd.read_csv(simu_path, dtype={"trade_date": str}).set_index("trade_date")
            ret_data[src_signal_id] = simu_df["netRet"]
        self.signal_simu_ret_df = pd.DataFrame(ret_data)
        return 0

    def __load_train_dates(self, bgn_date: str, stp_date: str) -> list[tuple[str, str, str]]:
        train_dates = []
        iter_months = self.calendar.map_iter_dates_to_iter_months2(bgn_date, stp_date)
        for __train_end_month in iter_months:
            __train_bgn_date, __train_end_date = self.calendar.get_bgn_and_end_dates_for_trailing_window(__train_end_month, self.trn_win)
            train_dates.append((__train_end_month, __train_bgn_date, __train_end_date))
        return train_dates

    def __get_selected_ret_df(self, bgn_date: str, end_date: str) -> pd.DataFrame:
        filter_dates = (self.signal_simu_ret_df.index >= bgn_date) & (self.signal_simu_ret_df.index <= end_date)
        return self.signal_simu_ret_df.loc[filter_dates]

    def _optimize(self, mu: pd.Series, sgm: pd.DataFrame) -> (np.ndarray, float):
        pass

    def __save(self, update_df: pd.DataFrame, run_mode: str):
        lib_writer = self.__get_optimized_lib_writer(run_mode)
        lib_writer.update(update_df)
        lib_writer.close()
        return 0

    def main(self, run_mode: str, bgn_date: str, stp_date: str):
        train_dates = self.__load_train_dates(bgn_date, stp_date)
        if train_dates:
            if self.__check_continuity(run_mode, train_dates[0][0]) == 0:
                self.__load_simu_tests()
                _a = 252
                model_data = {}
                for train_end_month, train_bgn_date, train_end_date in train_dates:
                    ret_df = self.__get_selected_ret_df(train_bgn_date, train_end_date)
                    if len(ret_df) < self.min_model_days:
                        ws = self.default_weights
                    else:
                        mu, sgm = ret_df.mean() * _a, ret_df.cov() * _a
                        w, _ = self._optimize(mu, sgm)
                        ws = pd.Series(data=w, index=mu.index)
                    model_data[train_end_date] = ws
                optimized_df = pd.DataFrame.from_dict(model_data, orient="index")
                update_df = optimized_df.stack(dropna=False).reset_index()
                self.__save(update_df, run_mode)
        return 0

    def get_signal_weight(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        header = pd.DataFrame({"trade_date": self.calendar.get_iter_list(bgn_date, stp_date, True)})
        lib_reader = self.__get_optimized_lib_reader()
        optimized_df = lib_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "signal", "value"])
        monthly_df = pd.pivot_table(data=optimized_df, index="trade_date", columns="signal", values="value")
        signal_weight_df = pd.merge(left=header, right=monthly_df, left_on="trade_date", right_index=True, how="left")
        signal_weight_df.set_index("trade_date", inplace=True)
        signal_weight_df = signal_weight_df.fillna(method="ffill").fillna(method="bfill")
        return signal_weight_df


class CSignalOptimizerMinUty(CSignalOptimizer):
    def _optimize(self, mu: pd.Series, sgm: pd.DataFrame) -> (np.ndarray, float):
        return minimize_utility(mu=mu.values, sigma=sgm.values, lbd=self.lbd)


class CSignalOptimizerMinUtyCon(CSignalOptimizer):
    def __init__(self, weight_bounds: tuple[float, float], total_pos_lim: tuple[float, float], maxiter: int, **kwargs):
        self.weight_bounds = weight_bounds
        self.total_pos_lim = total_pos_lim
        self.maxiter = maxiter
        super().__init__(**kwargs)

    def _optimize(self, mu: pd.Series, sgm: pd.DataFrame) -> (np.ndarray, float):
        return minimize_utility_con(mu=mu.values, sigma=sgm.values, lbd=self.lbd,
                                    bounds=self.weight_bounds, pos_lim=self.total_pos_lim,
                                    maxiter=self.maxiter)
