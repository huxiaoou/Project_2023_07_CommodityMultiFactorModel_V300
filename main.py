import argparse
import datetime as dt
from returns.instrument_return import merge_instru_return
from returns.available_universe import cal_available_universe
from returns.market_return import cal_market_return
from returns.test_return import cal_test_return
from returns.test_return_neutral import cal_test_return_neutral
from factors.factors_MTM import CFactorMTM
from factors.factors_transformer import CMpTransformerSum

from factors.factors_neutral import cal_factors_neutral_mp
from factors.factors_normalize_delinear import cal_factors_normalize_and_delinear_mp
from factors.factors_return import cal_factors_return_mp
from signals.signals_pure_factors_VANILLA import cal_signals_vanilla_mp
from signals.signals_pure_factors_MA import cal_signals_ma_mp
from signals.signals_portfolio_allocation_raw import cal_signals_raw_mp
from signals.signals_portfolio_allocation_pure import cal_signals_pure_mp
from signals.signals_opt_mov_ave import cal_signals_opt_vanilla_mp, cal_signals_opt_ma_mp, cal_signals_opt_raw_and_pure_mp
from ic_tests.ic_tests_factors import cal_ic_tests_mp
from ic_tests.ic_tests_factors_neutral import cal_ic_tests_neutral_mp
from ic_tests.ic_tests_factors_delinear import cal_ic_tests_delinear_mp
from ic_tests.ic_tests_summary import cal_ic_tests_summary_mp
from ic_tests.ic_tests_summary_neutral import cal_ic_tests_neutral_summary_mp
from ic_tests.ic_tests_summary_delinear import cal_ic_tests_delinear_summary_mp
from ic_tests.ic_tests_comparison import cal_ic_tests_comparison
from ic_tests.factors_exposure_corr import cal_factors_exposure_corr
from simulations.simulation import cal_simulations_mp
from simulations.evaluation import cal_evaluation_signals_mp
from simulations.evaluation_by_year import evaluate_signal_by_year, plot_signals_nav_by_year
from simulations.evaluation_positions_and_trades import cal_positions_and_trades_mp

from setup_model import (futures_by_instrument_dir, major_return_db_name, major_minor_db_name,
                         instrument_volume_db_name, instrument_member_db_name,
                         md_by_instru_dir, fundamental_by_instru_dir,
                         instruments_return_dir, available_universe_dir,
                         test_return_dir, test_return_neutral_dir,
                         factors_exposure_dir, factors_exposure_neutral_dir,
                         factors_exposure_norm_dir, factors_exposure_delinear_dir,
                         factors_return_dir, factors_portfolio_dir, instruments_residual_dir,
                         signals_dir, signals_allocation_dir, signals_opt_dir,
                         ic_tests_dir, ic_tests_delinear_dir, factors_exposure_corr_dir,
                         simulations_opt_dir, evaluations_opt_dir, by_year_dir, simu_positions_and_trades_dir,
                         calendar_path, instrument_info_path)
from config_factor import (bgn_dates_in_overwrite_mod, concerned_instruments_universe, sector_classification, sectors,
                           available_universe_options, test_windows, factors, neutral_method,
                           factors_pool_options, factors_return_lags,
                           windows_mtm, )
from config_portfolio import (available_factors, timing_factors,
                              pid, factors_return_lag, fast_n_slow_n_comb, raw_portfolio_options, pure_portfolio_options,
                              minimum_abs_weight, test_signals,
                              selected_sectors, selected_factors,
                              cost_rate, cost_reservation, init_premium, risk_free_rate)
from struct_lib_portfolio import database_structure
from skyrim.whiterun import CCalendar, CInstrumentInfoTable

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Entry point of this project", formatter_class=argparse.RawTextHelpFormatter)
    args_parser.add_argument("-w", "--switch", type=str,
                             help="""use this to decide which parts to run, available options = {
    'ir': instrument return,
    'au': available universe,
    'mr': market return,
    'tr': test return,
    'trn': test return neutral,
    'fe': factors exposure,
    'fen': factors exposure neutral,
    'dln': norm and delinear,
    'fr': factor return,
    'sigv': signal vanilla,
    'sigm': signal moving average for timing,
    'sigar': signal allocation raw,
    'sigap': signal allocation pure,
    'optv': optimization for signal vanilla,
    'optm': optimization for signal moving average,
    'opt': optimization for allocation,
    'ic': ic-tests,
    'icn': ic-tests-neutral,
    'icd': ic-tests-delinear,
    'ics': ic-tests-summary,
    'icns': ic-tests-neutral-summary,
    'icds': ic-tests-delinear-summary,
    'fecor': factor exposure correlation,
    'simu': simulation,
    'eval': evaluation,
    'by': evaluation by year,
    }""")
    args_parser.add_argument("-m", "--mode", type=str, choices=("o", "a"), help="""run mode, available options = {'o', 'a'}""")
    args_parser.add_argument("-b", "--bgn", type=str, help="""begin date, must be provided if run_mode = 'a' else DO NOT provided.""")
    args_parser.add_argument("-s", "--stp", type=str, help="""stop  date, NOT INCLUDED, must be provided if run_mode = 'o'.""")
    args_parser.add_argument("-p", "--process", type=int, default=5, help="""number of process to be called when calculating, default = 5""")
    args_parser.add_argument("-f", "--factor", type=str, default="",
                             help="""optional, must be provided if switch = 'factors_exposure', use this to decide which factor, available options = {
    'basis', 'beta',
    'csp', 'csr',
    'ctp', 'ctr',
    'cv',
    'cvp', 'cvr',
    'hp',
    'mtm',
    'rsw',
    'sgm',
    'size', 'skew', 'to',
    'ts',
    'vol',
    }""")
    args_parser.add_argument("-t", "--type", type=str, choices=("v", "m", "a"),
                             help="""
    v = portfolios with signals derived from vanilla pure factors
    m = portfolios with signals derived from pure factors with timing, methods = moving average
    a = allocations, both raw and pure
    """)
    args_parser.add_argument("-e", "--exeDate", type=str, help="""format = [YYYYMMDD], used if switch = 'POS', to calculate the positions and trades""")

    args = args_parser.parse_args()
    switch = args.switch.upper()
    if switch in ["ICS", "ICNS", "ICDS", "ICC", "FECOR", "SIMU", "EVAL", "BY", "POS"]:
        run_mode = None
    elif switch in ["MR"]:
        run_mode = "O"
    else:
        run_mode = args.mode.upper()
    bgn_date, stp_date = (bgn_dates_in_overwrite_mod[switch] if run_mode in ["O"] else args.bgn), args.stp
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    proc_num = args.process
    factor = args.factor.upper() if switch in ["FE"] else None
    signals_type = args.type.upper() if switch in ["SIMU", "EVAL"] else None
    exe_date = args.exeDate.upper() if switch in ["POS"] else None

    # some shared data
    calendar = CCalendar(calendar_path)
    instru_into_tab = CInstrumentInfoTable(instrument_info_path, t_index_label="windCode", t_type="CSV")

    if switch in ["IR"]:  # "INSTRUMENT RETURN":
        merge_instru_return(
            bgn_date=bgn_date, stp_date=stp_date,
            futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name,
            instruments_return_dir=instruments_return_dir,
            concerned_instruments_universe=concerned_instruments_universe,
        )
    elif switch in ["AU"]:  # "AVAILABLE UNIVERSE"
        cal_available_universe(
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            instruments_universe=concerned_instruments_universe,
            available_universe_options=available_universe_options,
            available_universe_dir=available_universe_dir,
            futures_by_instrument_dir=futures_by_instrument_dir,
            major_return_db_name=major_return_db_name,
            database_structure=database_structure,
            calendar=calendar,
        )
    elif switch in ["MR"]:  # "MARKET RETURN"
        cal_market_return(
            bgn_date=bgn_date, stp_date=stp_date,
            available_universe_dir=available_universe_dir,
            instruments_return_dir=instruments_return_dir,
            database_structure=database_structure,
            calendar=calendar,
        )
    elif switch in ["TR"]:  # "TEST RETURN"
        cal_test_return(
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            instruments_return_dir=instruments_return_dir,
            test_return_dir=test_return_dir,
            database_structure=database_structure,
            calendar=calendar,
        )
    elif switch in ["TRN"]:  # "TEST RETURN NEUTRAL"
        cal_test_return_neutral(
            neutral_method=neutral_method,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            instruments_universe=concerned_instruments_universe,
            available_universe_dir=available_universe_dir,
            sector_classification=sector_classification,
            test_return_dir=test_return_dir,
            test_return_neutral_dir=test_return_neutral_dir,
            database_structure=database_structure,
            calendar=calendar,
        )
    elif switch in ["FE"]:
        if factor == "MTM":
            agent_factor = CFactorMTM(
                futures_by_instrument_dir=futures_by_instrument_dir,
                major_return_db_name=major_return_db_name,
                concerned_instruments_universe=concerned_instruments_universe,
                factors_exposure_dir=factors_exposure_dir,
                database_structure=database_structure,
                calendar=calendar)
            agent_factor.core(run_mode=run_mode, bgn_date=bgn_dates_in_overwrite_mod["FEB"] if run_mode in ["O"] else bgn_date, stp_date=stp_date)
            agent_mp = CMpTransformerSum(proc_num=proc_num, arg_wins=windows_mtm, src_factor_id="MTM", run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)
            agent_mp.mp_cal_transform(concerned_instruments_universe=concerned_instruments_universe,
                                      factors_exposure_dir=factors_exposure_dir,
                                      database_structure=database_structure,
                                      calendar=calendar)

    # elif switch in ["FEN"]:
    #     cal_factors_neutral_mp(
    #         proc_num=proc_num, factors=factors,
    #         neutral_method=neutral_method,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         concerned_instruments_universe=concerned_instruments_universe,
    #         sector_classification=sector_classification,
    #         available_universe_dir=available_universe_dir,
    #         factors_exposure_dir=factors_exposure_dir,
    #         factors_exposure_neutral_dir=factors_exposure_neutral_dir,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["DLN"]:
    #     cal_factors_normalize_and_delinear_mp(
    #         proc_num=proc_num, pids=list(factors_pool_options.keys()),
    #         selected_factors_pool=factors_pool_options["P3"],
    #         neutral_method=neutral_method,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         concerned_instruments_universe=concerned_instruments_universe,
    #         sector_classification=sector_classification,
    #         available_universe_dir=available_universe_dir,
    #         factors_exposure_dir=factors_exposure_dir,
    #         factors_exposure_norm_dir=factors_exposure_norm_dir,
    #         factors_exposure_delinear_dir=factors_exposure_delinear_dir,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["FR"]:
    #     cal_factors_return_mp(
    #         proc_num=proc_num, pids=["P3"], factors_pool_options=factors_pool_options,
    #         neutral_methods=["WS"], test_windows=test_windows, factors_return_lags=factors_return_lags,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         sectors=sectors, sector_classification=sector_classification,
    #         concerned_instruments_universe=concerned_instruments_universe,
    #         available_universe_dir=available_universe_dir,
    #         factors_exposure_delinear_dir=factors_exposure_delinear_dir, test_return_dir=test_return_dir,
    #         factors_return_dir=factors_return_dir, factors_portfolio_dir=factors_portfolio_dir,
    #         instruments_residual_dir=instruments_residual_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["SIGV"]:
    #     cal_signals_vanilla_mp(
    #         proc_num=proc_num,
    #         test_windows=test_windows, pids=[pid], neutral_methods=[neutral_method], factors_return_lags=[factors_return_lag],
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         src_factors=available_factors,
    #         factors_portfolio_dir=factors_portfolio_dir,
    #         signals_dir=signals_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["SIGM"]:
    #     cal_signals_ma_mp(
    #         proc_num=proc_num,
    #         test_windows=test_windows, pids=[pid], neutral_methods=[neutral_method], factors_return_lags=[factors_return_lag],
    #         fast_n_slow_n_comb=fast_n_slow_n_comb,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         src_factors=timing_factors,
    #         factors_return_dir=factors_return_dir,
    #         factors_portfolio_dir=factors_portfolio_dir,
    #         signals_dir=signals_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["SIGAR"]:
    #     cal_signals_raw_mp(
    #         proc_num=proc_num,
    #         raw_portfolio_ids=list(raw_portfolio_options), raw_portfolio_options=raw_portfolio_options,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         test_universe=concerned_instruments_universe,
    #         available_universe_dir=available_universe_dir,
    #         factors_exposure_dir=factors_exposure_dir,
    #         signals_allocation_dir=signals_allocation_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["SIGAP"]:
    #     cal_signals_pure_mp(
    #         proc_num=proc_num,
    #         pure_portfolio_ids=list(pure_portfolio_options), pure_portfolio_options=pure_portfolio_options,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         signals_dir=signals_dir,
    #         signals_allocation_dir=signals_allocation_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["OPTV"]:
    #     cal_signals_opt_vanilla_mp(
    #         proc_num=proc_num,
    #         factors=available_factors,
    #         mov_ave_lens=test_windows,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         minimum_abs_weight=minimum_abs_weight,
    #         src_dir=signals_dir,
    #         signals_opt_dir=signals_opt_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["OPTM"]:
    #     cal_signals_opt_ma_mp(
    #         proc_num=proc_num,
    #         factors=timing_factors,
    #         mov_ave_lens=test_windows,
    #         fast_n_slow_n_comb=fast_n_slow_n_comb,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         minimum_abs_weight=minimum_abs_weight,
    #         src_dir=signals_dir,
    #         signals_opt_dir=signals_opt_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["OPT"]:
    #     cal_signals_opt_raw_and_pure_mp(
    #         proc_num=proc_num,
    #         portfolio_ids=list(raw_portfolio_options) + list(pure_portfolio_options),
    #         mov_ave_lens=test_windows,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         minimum_abs_weight=minimum_abs_weight,
    #         src_dir=signals_allocation_dir,
    #         signals_opt_dir=signals_opt_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["IC"]:
    #     cal_ic_tests_mp(
    #         proc_num=proc_num,
    #         factors=factors, test_windows=test_windows,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         ic_tests_dir=ic_tests_dir,
    #         available_universe_dir=available_universe_dir,
    #         exposure_dir=factors_exposure_dir,
    #         return_dir=test_return_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["ICN"]:
    #     cal_ic_tests_neutral_mp(
    #         proc_num=proc_num,
    #         factors=factors, neutral_methods=[neutral_method], test_windows=test_windows,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         ic_tests_dir=ic_tests_dir,
    #         available_universe_dir=available_universe_dir,
    #         exposure_dir=factors_exposure_neutral_dir,
    #         return_dir=test_return_neutral_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["ICD"]:
    #     cal_ic_tests_delinear_mp(
    #         proc_num=proc_num,
    #         pids=[pid], factors_pool_options=factors_pool_options,
    #         neutral_methods=[neutral_method], test_windows=test_windows, factors_return_lags=[0],
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         ic_tests_delinear_dir=ic_tests_delinear_dir,
    #         exposure_dir=factors_exposure_delinear_dir,
    #         return_dir=test_return_neutral_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["ICS"]:
    #     cal_ic_tests_summary_mp(
    #         proc_num=proc_num, factors=factors,
    #         test_windows=test_windows,
    #         ic_tests_dir=ic_tests_dir,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["ICNS"]:
    #     cal_ic_tests_neutral_summary_mp(
    #         proc_num=proc_num, factors=factors, neutral_methods=[neutral_method],
    #         test_windows=test_windows,
    #         ic_tests_dir=ic_tests_dir,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["ICDS"]:
    #     cal_ic_tests_delinear_summary_mp(
    #         proc_num=proc_num,
    #         pids=[pid], factors_pool_options=factors_pool_options,
    #         neutral_methods=[neutral_method], test_windows=test_windows, factors_return_lags=[0],
    #         ic_tests_delinear_dir=ic_tests_delinear_dir,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["ICC"]:
    #     cal_ic_tests_comparison(
    #         factors=factors, neutral_method=neutral_method, exception_list=[],
    #         ic_tests_dir=ic_tests_dir, top_n=12
    #     )
    # elif switch in ["FECOR"]:
    #     test_factor_list_l, test_factor_list_r = ["MTM231", "TS126"], []
    #     cal_factors_exposure_corr(
    #         neutral_method=neutral_method,
    #         test_factor_list_l=test_factor_list_l, test_factor_list_r=test_factor_list_r,
    #         bgn_date=bgn_date, stp_date=stp_date,
    #         factors_exposure_dir=factors_exposure_dir,
    #         factors_exposure_neutral_dir=factors_exposure_neutral_dir,
    #         factors_exposure_corr_dir=factors_exposure_corr_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["SIMU"]:
    #     if signals_type == "V":
    #         signal_ids = test_signals["vanilla"]
    #     elif signals_type == "M":
    #         signal_ids = test_signals["ma"]
    #     elif signals_type == "A":
    #         signal_ids = test_signals["allocation"]
    #     else:
    #         signal_ids = []
    #     cal_simulations_mp(
    #         proc_num=proc_num,
    #         signal_ids=signal_ids, test_windows=[1],
    #         calendar_path=calendar_path, instrument_info_path=instrument_info_path,
    #         md_by_instru_dir=md_by_instru_dir, major_minor_dir=major_minor_dir,
    #         available_universe_dir=available_universe_dir,
    #         sig_dir=signals_opt_dir, dst_dir=simulations_opt_dir,
    #         database_structure=database_structure,
    #         test_universe=concerned_instruments_universe, test_bgn_date=bgn_date, test_stp_date=stp_date,
    #         cost_reservation=cost_reservation, cost_rate=cost_rate, init_premium=init_premium,
    #         skip_if_exist=False
    #     )
    # elif switch in ["EVAL"]:
    #     if signals_type == "V":
    #         signal_ids = test_signals["vanilla"]
    #     elif signals_type == "M":
    #         signal_ids = test_signals["ma"]
    #     elif signals_type == "A":
    #         signal_ids = test_signals["allocation"]
    #     else:
    #         signal_ids = []
    #     cal_evaluation_signals_mp(
    #         proc_num=proc_num,
    #         signal_ids=signal_ids,
    #         hold_period_n_list=[1], bgn_date=bgn_date, stp_date=stp_date,
    #         risk_free_rate=risk_free_rate,
    #         src_simulations_dir=simulations_opt_dir,
    #         dst_evaluations_dir=evaluations_opt_dir,
    #         top_n=5, verbose=False,
    #     )
    # elif switch in ["BY"]:
    #     evaluate_signal_by_year("R1M010", 1, risk_free_rate, evaluations_opt_dir, by_year_dir)
    #     evaluate_signal_by_year("R4M010", 1, risk_free_rate, evaluations_opt_dir, by_year_dir)
    #     evaluate_signal_by_year("A1M020", 1, risk_free_rate, evaluations_opt_dir, by_year_dir)
    #     evaluate_signal_by_year("A6M005", 1, risk_free_rate, evaluations_opt_dir, by_year_dir)
    #     evaluate_signal_by_year("A3M020", 1, risk_free_rate, evaluations_opt_dir, by_year_dir)
    #     evaluate_signal_by_year("A8M005", 1, risk_free_rate, evaluations_opt_dir, by_year_dir)
    #     tracking_sigal_ids = [
    #         "R1M010",
    #         "R4M010",
    #         "A1M020",
    #         "A6M005",
    #         "A3M020",
    #         "A8M005",
    #     ]
    #     plot_signals_nav_by_year("comb", [(z, 1) for z in tracking_sigal_ids], evaluations_opt_dir, by_year_dir)
    #     plot_signals_nav_by_year("comb_sector_VM005", [(z + "VM005", 1) for z in ["MARKET"] + selected_sectors], evaluations_opt_dir, by_year_dir)
    #     plot_signals_nav_by_year("comb_sector_VM020", [(z + "VM020", 1) for z in ["MARKET"] + selected_sectors], evaluations_opt_dir, by_year_dir)
    #     plot_signals_nav_by_year("comb_style_VM005", [(z + "VM005", 1) for z in selected_factors], evaluations_opt_dir, by_year_dir)
    #     plot_signals_nav_by_year("comb_style_VM020", [(z + "VM020", 1) for z in selected_factors], evaluations_opt_dir, by_year_dir)
    # elif switch in ["POS"]:
    #     cal_positions_and_trades_mp(
    #         proc_num=proc_num,
    #         sids=["R1M010", "R4M010", "A1M020", "A6M005", "A3M020", "A8M005"],
    #         exe_date=exe_date,
    #         init_premium=init_premium,
    #         instruments_universe=concerned_instruments_universe,
    #         signals_opt_dir=signals_opt_dir,
    #         md_by_instru_dir=md_by_instru_dir,
    #         major_minor_dir=major_minor_dir,
    #         simu_positions_and_trades_dir=simu_positions_and_trades_dir,
    #         calendar_path=calendar_path,
    #         instru_info_tab_path=instrument_info_path,
    #         database_structure=database_structure,
    #     )
    else:
        print(f"... switch = {switch} is not a legal option, please check again.")
