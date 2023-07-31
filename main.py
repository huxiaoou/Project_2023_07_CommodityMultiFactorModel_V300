import argparse
import datetime as dt
from returns.instrument_return import merge_instru_return
from returns.available_universe import cal_available_universe
from returns.market_return import cal_market_return
from returns.test_return import cal_test_return_mp
from returns.test_return_neutral import cal_test_return_neutral_mp
from factors.factors_algorithm_BASIS import cal_factors_exposure_basis_mp
from factors.factors_algorithm_BETA import cal_factors_exposure_beta_mp
from factors.factors_algorithm_CSP import cal_factors_exposure_csp_mp
from factors.factors_algorithm_CSR import cal_factors_exposure_csr_mp
from factors.factors_algorithm_CTP import cal_factors_exposure_ctp_mp
from factors.factors_algorithm_CTR import cal_factors_exposure_ctr_mp
from factors.factors_algorithm_CV import cal_factors_exposure_cv_mp
from factors.factors_algorithm_CVP import cal_factors_exposure_cvp_mp
from factors.factors_algorithm_CVR import cal_factors_exposure_cvr_mp
from factors.factors_algorithm_HP import cal_factors_exposure_hp_mp
from factors.factors_algorithm_MTM import cal_factors_exposure_mtm_mp
from factors.factors_algorithm_RSW import cal_factors_exposure_rsw_mp
from factors.factors_algorithm_SGM import cal_factors_exposure_sgm_mp
from factors.factors_algorithm_SIZE import cal_factors_exposure_size_mp
from factors.factors_algorithm_SKEW import cal_factors_exposure_skew_mp
from factors.factors_algorithm_TO import cal_factors_exposure_to_mp
from factors.factors_algorithm_TS import cal_factors_exposure_ts_mp
from factors.factors_algorithm_VOL import cal_factors_exposure_vol_mp
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
from config_factor import (concerned_instruments_universe, sector_classification, sectors,
                           available_universe_options, test_windows, factors_args, factors, neutral_method,
                           factors_pool_options, factors_return_lags)
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
    args_parser.add_argument("-m", "--mode", type=str, choices=("o", "a"),
                             help="""run mode, available options = {'o', 'overwrite', 'a', 'append'}""")
    args_parser.add_argument("-b", "--bgn", type=str,
                             help="""begin date, may be different according to different switches, suggestion of different switch:
    {
        "returns/instrument_return": "20120101",
        
        "returns/available_universe": "20120301",
        "returns/market_return": None,
        "returns/test_return": "20120301",
        "returns/test_return_neutral": "20120301",
        
        "factors/exposure": "20130101",
        "factors/exposure_neutral": "20130101",
        
        "factors/exposure_norm_and_delinear": "20130201", # some factors with a large window (252) would start at about this time
        
        "factors/return": "20140101",
        "signals/VANILLA": "20140101",
        "signals/MA": "20140101",
        "signals/allocation_raw": "20140101",
        "signals/allocation_pure": "20140101",
        
        "signals/opt_raw_pure": "20140301",
        "signals/opt_vanilla": "20140301",
        "signals/opt_ma": "20140301",
        
        "ic_tests/": "20140101",
        "ic_tests/neutral": "20140101",
        "ic_tests/delinear": "20140101",
        "ic_tests/fecor": "20140101",
        
        "simulation": "20140701",
    }""")
    args_parser.add_argument("-s", "--stp", type=str,
                             help="""    stop date, not included, usually it would be the day after the last trade date, such as
    "20230619" if last trade date is "20230616" """)
    args_parser.add_argument("-p", "--process", type=int, default=5,
                             help="""    number of process to be called when calculating, default = 5""")
    args_parser.add_argument("-f", "--factor", type=str, default="",
                             help="""    optional, must be provided if switch = {'factors_exposure'},
    use this to decide which factor, available options = {
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
                             help="""    v = portfolios with signals derived from vanilla pure factors
    m = portfolios with signals derived from pure factors with timing, methods = moving average
    a = allocations, both raw and pure
    """)
    args_parser.add_argument("-e", "--exeDate", type=str,
                             help="""   format = [YYYYMMDD], used if switch = 'POS', to calculate the positions and trades""")

    args = args_parser.parse_args()
    switch = args.switch.upper()
    run_mode = None if switch in ["IR", "MR",
                                  "ICS", "ICNS", "ICDS", "ICC", "FECOR",
                                  "SIMU", "EVAL", "BY", "POS"] else args.mode.upper()
    bgn_date, stp_date = args.bgn, args.stp
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    proc_num = args.process
    factor = args.factor.upper() if switch in ["FE"] else None
    signals_type = args.type.upper() if switch in ["SIMU", "EVAL"] else None
    exe_date = args.exeDate.upper() if switch in ["POS"] else None

    #
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
    # elif switch in ["MR"]:  # "MARKET RETURN"
    #     cal_market_return(
    #         instruments_return_dir=instruments_return_dir,
    #         available_universe_dir=available_universe_dir,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["TR"]:  # "TEST RETURN"
    #     cal_test_return_mp(
    #         proc_num=proc_num,
    #         test_windows=test_windows,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         instruments_return_dir=instruments_return_dir,
    #         test_return_dir=test_return_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure
    #     )
    # elif switch in ["TRN"]:  # "TEST RETURN NEUTRAL"
    #     cal_test_return_neutral_mp(
    #         proc_num=proc_num,
    #         test_windows=test_windows, neutral_method=neutral_method,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         instruments_universe=concerned_instruments_universe,
    #         available_universe_dir=available_universe_dir,
    #         sector_classification=sector_classification,
    #         test_return_dir=test_return_dir,
    #         test_return_neutral_dir=test_return_neutral_dir,
    #         calendar_path=calendar_path,
    #         database_structure=database_structure,
    #     )
    # elif switch in ["FE"]:
    #     if factor == "BASIS":
    #         cal_factors_exposure_basis_mp(proc_num=proc_num, basis_windows=factors_args["BASIS"],
    #                                       run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                       concerned_instruments_universe=concerned_instruments_universe,
    #                                       factors_exposure_dir=factors_exposure_dir,
    #                                       fundamental_dir=fundamental_by_instru_dir,
    #                                       major_minor_dir=major_minor_dir,
    #                                       calendar_path=calendar_path,
    #                                       database_structure=database_structure,
    #                                       )
    #     if factor == "BETA":
    #         cal_factors_exposure_beta_mp(proc_num=proc_num, beta_windows=factors_args["BETA"],
    #                                      run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                      concerned_instruments_universe=concerned_instruments_universe,
    #                                      factors_exposure_dir=factors_exposure_dir,
    #                                      instruments_return_dir=instruments_return_dir,
    #                                      major_return_dir=major_return_dir,
    #                                      calendar_path=calendar_path,
    #                                      database_structure=database_structure,
    #                                      )
    #     if factor == "CSP":
    #         cal_factors_exposure_csp_mp(proc_num=proc_num, csp_windows=factors_args["CSP"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "CSR":
    #         cal_factors_exposure_csr_mp(proc_num=proc_num, csr_windows=factors_args["CSR"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "CTP":
    #         cal_factors_exposure_ctp_mp(proc_num=proc_num, ctp_windows=factors_args["CTP"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "CTR":
    #         cal_factors_exposure_ctr_mp(proc_num=proc_num, ctr_windows=factors_args["CTR"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "CV":
    #         cal_factors_exposure_cv_mp(proc_num=proc_num, cv_windows=factors_args["CV"],
    #                                    run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                    concerned_instruments_universe=concerned_instruments_universe,
    #                                    factors_exposure_dir=factors_exposure_dir,
    #                                    major_return_dir=major_return_dir,
    #                                    calendar_path=calendar_path,
    #                                    database_structure=database_structure,
    #                                    )
    #     if factor == "CVP":
    #         cal_factors_exposure_cvp_mp(proc_num=proc_num, cvp_windows=factors_args["CVP"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "CVR":
    #         cal_factors_exposure_cvr_mp(proc_num=proc_num, cvr_windows=factors_args["CVR"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "HP":
    #         cal_factors_exposure_hp_mp(proc_num=proc_num, hp_windows=factors_args["HP"],
    #                                    run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                    concerned_instruments_universe=concerned_instruments_universe,
    #                                    factors_exposure_dir=factors_exposure_dir,
    #                                    major_return_dir=major_return_dir,
    #                                    calendar_path=calendar_path,
    #                                    database_structure=database_structure,
    #                                    )
    #     if factor == "MTM":
    #         cal_factors_exposure_mtm_mp(proc_num=proc_num, mtm_windows=factors_args["MTM"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "RSW":
    #         cal_factors_exposure_rsw_mp(proc_num=proc_num, rsw_windows=[252], half_life_windows=factors_args["RSW252HL"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     fundamental_dir=fundamental_by_instru_dir,
    #                                     major_minor_dir=major_minor_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "SGM":
    #         cal_factors_exposure_sgm_mp(proc_num=proc_num, sgm_windows=factors_args["SGM"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
    #     if factor == "SIZE":
    #         cal_factors_exposure_size_mp(proc_num=proc_num, size_windows=factors_args["SIZE"],
    #                                      run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                      concerned_instruments_universe=concerned_instruments_universe,
    #                                      factors_exposure_dir=factors_exposure_dir,
    #                                      major_return_dir=major_return_dir,
    #                                      calendar_path=calendar_path,
    #                                      database_structure=database_structure,
    #                                      )
    #     if factor == "SKEW":
    #         cal_factors_exposure_skew_mp(proc_num=proc_num, skew_windows=factors_args["SKEW"],
    #                                      run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                      concerned_instruments_universe=concerned_instruments_universe,
    #                                      factors_exposure_dir=factors_exposure_dir,
    #                                      major_return_dir=major_return_dir,
    #                                      calendar_path=calendar_path,
    #                                      database_structure=database_structure,
    #                                      )
    #     if factor == "TO":
    #         cal_factors_exposure_to_mp(proc_num=proc_num, to_windows=factors_args["TO"],
    #                                    run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                    concerned_instruments_universe=concerned_instruments_universe,
    #                                    factors_exposure_dir=factors_exposure_dir,
    #                                    major_return_dir=major_return_dir,
    #                                    calendar_path=calendar_path,
    #                                    database_structure=database_structure,
    #                                    )
    #     if factor == "TS":
    #         cal_factors_exposure_ts_mp(proc_num=proc_num, ts_windows=factors_args["TS"],
    #                                    run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                    concerned_instruments_universe=concerned_instruments_universe,
    #                                    factors_exposure_dir=factors_exposure_dir,
    #                                    major_minor_dir=major_minor_dir,
    #                                    md_dir=md_by_instru_dir,
    #                                    calendar_path=calendar_path,
    #                                    database_structure=database_structure,
    #                                    )
    #     if factor == "VOL":
    #         cal_factors_exposure_vol_mp(proc_num=proc_num, vol_windows=factors_args["VOL"],
    #                                     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #                                     concerned_instruments_universe=concerned_instruments_universe,
    #                                     factors_exposure_dir=factors_exposure_dir,
    #                                     major_return_dir=major_return_dir,
    #                                     calendar_path=calendar_path,
    #                                     database_structure=database_structure,
    #                                     )
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
