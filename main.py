import argparse
import pandas as pd
import datetime as dt
from factors.factors_normalize_delinear import cal_factors_normalize_and_delinear_mp
from factors.factors_return import cal_factors_return_mp
from ic_tests.factors_exposure_corr import cal_factors_exposure_corr

from setup_model import (macro_economic_dir, cpi_m2_file, forex_dir, exchange_rate_file,
                         futures_by_instrument_dir, major_return_db_name, major_minor_db_name,
                         instrument_volume_db_name, instrument_member_db_name,
                         md_by_instru_dir, fundamental_by_instru_dir,
                         instruments_return_dir, available_universe_dir,
                         test_return_dir, test_return_neutral_dir,
                         factors_exposure_dir, factors_exposure_neutral_dir,
                         factors_exposure_norm_dir, factors_exposure_delinear_dir,
                         factors_return_dir, factors_portfolio_dir, instruments_residual_dir,
                         signals_dir, signals_allocation_dir, signals_opt_dir,
                         ic_tests_dir, ic_tests_summary_dir, ic_tests_delinear_dir, factors_exposure_corr_dir,
                         simulations_opt_dir, evaluations_opt_dir, by_year_dir, simu_positions_and_trades_dir,
                         calendar_path, instrument_info_path)
from config_project import (bgn_dates_in_overwrite_mod, concerned_instruments_universe, sector_classification, sectors,
                            available_universe_options, neutral_method, test_windows, factors_pool_options, factors_return_lags, selected_pool)
from config_factor import factors_settings, factors, factors_classification, factors_group, factors_basis
from struct_lib_portfolio import database_structure
from skyrim.whiterun import CCalendarMonthly, CInstrumentInfoTable

from config_portfolio import (available_factors, timing_factors,
                              pid, factors_return_lag,
                              minimum_abs_weight,
                              selected_sectors, selected_factors,
                              cost_rate, cost_reservation, init_premium, risk_free_rate)

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
    'MTM', SIZE','OI', 'RS', 'BASIS','TS','LIQUID','SR','HR','NETOI','NETOIW','NETDOI','NETDOIW',
    'SKEW','VOL','RVOL','CV','CTP','CVP','CSP','BETA','VAL','CBETA','IBETA','MACD','KDJ','RSI',  
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
    elif switch in ["IR", "MR"]:
        run_mode = "O"
    else:
        run_mode = args.mode.upper()
    bgn_date, stp_date = (bgn_dates_in_overwrite_mod[switch] if run_mode in ["O"] else args.bgn), args.stp
    if (stp_date is None) and (bgn_date is not None):
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    proc_num = args.process
    factor = args.factor.upper() if switch in ["FE"] else None
    signals_type = args.type.upper() if switch in ["SIMU", "EVAL"] else None
    exe_date = args.exeDate.upper() if switch in ["POS"] else None

    # some shared data
    calendar = CCalendarMonthly(calendar_path)
    instru_into_tab = CInstrumentInfoTable(instrument_info_path, t_index_label="windCode", t_type="CSV")
    mother_universe_df = pd.DataFrame({"instrument": concerned_instruments_universe})
    sector_df = pd.DataFrame.from_dict({z: {sector_classification[z]: 1} for z in concerned_instruments_universe}, orient="index").fillna(0)

    #  ----------- CORE -----------
    if switch in ["IR"]:  # "INSTRUMENT RETURN":
        from returns.instrument_return import merge_instru_return

        merge_instru_return(
            bgn_date=bgn_date, stp_date=stp_date,
            futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name,
            instruments_return_dir=instruments_return_dir,
            concerned_instruments_universe=concerned_instruments_universe,
        )
    elif switch in ["AU"]:  # "AVAILABLE UNIVERSE"
        from returns.available_universe import cal_available_universe

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
        from returns.market_return import cal_market_return

        cal_market_return(
            bgn_date=bgn_date, stp_date=stp_date,
            available_universe_dir=available_universe_dir,
            instruments_return_dir=instruments_return_dir,
            database_structure=database_structure,
            calendar=calendar,
        )
    elif switch in ["TR"]:  # "TEST RETURN"
        from returns.test_return import cal_test_return

        cal_test_return(
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            instruments_return_dir=instruments_return_dir,
            test_return_dir=test_return_dir,
            database_structure=database_structure,
            calendar=calendar,
        )
    elif switch in ["TRN"]:  # "TEST RETURN NEUTRAL"
        from returns.test_return_neutral import cal_test_return_neutral

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
        from factors.factors_cls_without_args import (CFactorMTM, CFactorsSIZE, CFactorsOI, CFactorsRS, CFactorsBASIS, CFactorsTS, CFactorsLIQUID,
                                                      CFactorsSR, CFactorsHR, CFactorsNETOI, CFactorsNETOIW, CFactorsNETDOI, CFactorsNETDOIW)
        from factors.factors_cls_with_args import CMpFactorWithArgWin, CMpFactorMACD, CMpFactorKDJ, CMpFactorRSI
        from factors.factors_cls_transformer import CMpTransformer

        shared_keywords = dict(concerned_instruments_universe=concerned_instruments_universe, factors_exposure_dst_dir=factors_exposure_dir,
                               database_structure=database_structure, calendar=calendar)
        raw_factor_bgn_date = bgn_dates_in_overwrite_mod["FEB"] if run_mode in ["O"] else bgn_date
        ewm_bgn_date = bgn_dates_in_overwrite_mod["FEB"]
        if factor == "MTM":
            agent_factor = CFactorMTM(futures_by_instrument_dir, major_return_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "SUM", factors_settings[factor]["S"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "SHARPE", factors_settings[factor]["SP"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "SIZE":
            agent_factor = CFactorsSIZE(futures_by_instrument_dir, instrument_volume_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BR", factors_settings[factor]["BR"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LR", factors_settings[factor]["LR"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "OI":
            agent_factor = CFactorsOI(futures_by_instrument_dir, instrument_volume_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "BR", factors_settings[factor]["BR"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LR", factors_settings[factor]["LR"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "RS":
            agent_factor = CFactorsRS(fundamental_by_instru_dir, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "BR", factors_settings[factor]["BR"], -1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LR", factors_settings[factor]["LR"], -1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "BASIS":
            agent_factor = CFactorsBASIS(fundamental_by_instru_dir, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "TS":
            agent_factor = CFactorsTS(futures_by_instrument_dir, major_minor_db_name, md_by_instru_dir, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "LIQUID":
            agent_factor = CFactorsLIQUID(futures_by_instrument_dir, major_return_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "SR":
            agent_factor = CFactorsSR(futures_by_instrument_dir, instrument_volume_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "HR":
            agent_factor = CFactorsHR(futures_by_instrument_dir, instrument_volume_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "NETOI":
            agent_factor = CFactorsNETOI(futures_by_instrument_dir, instrument_volume_db_name, instrument_member_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "NETOIW":
            agent_factor = CFactorsNETOIW(futures_by_instrument_dir, instrument_volume_db_name, instrument_member_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "NETDOI":
            agent_factor = CFactorsNETDOI(futures_by_instrument_dir, instrument_volume_db_name, instrument_member_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "NETDOIW":
            agent_factor = CFactorsNETDOIW(futures_by_instrument_dir, instrument_volume_db_name, instrument_member_db_name, **shared_keywords)
            agent_factor.core(run_mode, raw_factor_bgn_date, stp_date)
            agent_transformer = CMpTransformer(proc_num, [factor], "AVER", factors_settings[factor]["A"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "BD", factors_settings[factor]["BD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)
            agent_transformer = CMpTransformer(proc_num, [factor], "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor in ["SKEW", "VOL", "RVOL", "CV", "CTP", "CVP", "CSP", "VAL"]:
            agent_factor = CMpFactorWithArgWin(proc_num, factor, factors_settings[factor][""], run_mode, bgn_date, stp_date)
            agent_factor.mp_cal_factor(futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name, **shared_keywords)
            src_factor_ids = [f"{factor}{_:03d}" for _ in factors_settings[factor][""]]
            agent_transformer = CMpTransformer(proc_num, src_factor_ids, "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "BETA":
            agent_factor = CMpFactorWithArgWin(proc_num, factor, factors_settings[factor][""], run_mode, bgn_date, stp_date)
            agent_factor.mp_cal_factor(futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name,
                                       market_return_dir=instruments_return_dir, market_return_file="market.return.csv.gz", **shared_keywords)

            src_factor_ids = [f"{factor}{_:03d}" for _ in factors_settings[factor][""]]
            agent_transformer = CMpTransformer(proc_num, src_factor_ids, "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "CBETA":
            agent_factor = CMpFactorWithArgWin(proc_num, factor, factors_settings[factor][""], run_mode, bgn_date, stp_date)
            agent_factor.mp_cal_factor(futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name,
                                       forex_dir=forex_dir, exchange_rate_file=exchange_rate_file, **shared_keywords)

            src_factor_ids = [f"{factor}{_:03d}" for _ in factors_settings[factor][""]]
            agent_transformer = CMpTransformer(proc_num, src_factor_ids, "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "IBETA":
            agent_factor = CMpFactorWithArgWin(proc_num, factor, factors_settings[factor][""], run_mode, bgn_date, stp_date)
            agent_factor.mp_cal_factor(futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name,
                                       macro_economic_dir=macro_economic_dir, cpi_m2_file=cpi_m2_file, **shared_keywords)

            src_factor_ids = [f"{factor}{_:03d}" for _ in factors_settings[factor][""]]
            agent_transformer = CMpTransformer(proc_num, src_factor_ids, "LD", factors_settings[factor]["LD"], 1, factors_exposure_dir,
                                               run_mode, bgn_date, stp_date, factor)
            agent_transformer.mp_cal_transform(**shared_keywords)

        if factor == "MACD":
            agent_factor = CMpFactorMACD(proc_num, factors_settings[factor]["F"], factors_settings[factor]["S"], factors_settings[factor]["ALPHA"],
                                         ewm_bgn_date, run_mode, bgn_date, stp_date)
            agent_factor.mp_cal_factor(futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name, **shared_keywords)

        if factor == "KDJ":
            agent_factor = CMpFactorKDJ(proc_num, factors_settings[factor]["N"], ewm_bgn_date, run_mode, bgn_date, stp_date)
            agent_factor.mp_cal_factor(futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name, **shared_keywords)

        if factor == "RSI":
            agent_factor = CMpFactorRSI(proc_num, factors_settings[factor]["N"], ewm_bgn_date, run_mode, bgn_date, stp_date)
            agent_factor.mp_cal_factor(futures_by_instrument_dir=futures_by_instrument_dir, major_return_db_name=major_return_db_name, **shared_keywords)
    elif switch in ["FEN"]:
        from factors.factors_neutral import cal_factors_neutral_mp

        cal_factors_neutral_mp(
            proc_num=proc_num, factors=factors,
            neutral_method=neutral_method,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            mother_universe_df=mother_universe_df, sector_df=sector_df,
            available_universe_dir=available_universe_dir,
            factors_exposure_dir=factors_exposure_dir,
            factors_exposure_neutral_dir=factors_exposure_neutral_dir,
            database_structure=database_structure,
            calendar=calendar, )
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
    elif switch in ["IC"]:
        from ic_tests.ic_tests_cls import cal_ic_tests_mp

        cal_ic_tests_mp(
            proc_num=proc_num, factors=factors,
            ic_tests_dir=ic_tests_dir,
            available_universe_dir=available_universe_dir,
            exposure_dir=factors_exposure_dir,
            test_return_dir=test_return_dir,
            database_structure=database_structure, calendar=calendar,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            neutral_method="",
        )
    elif switch in ["ICN"]:
        from ic_tests.ic_tests_cls import cal_ic_tests_mp

        cal_ic_tests_mp(
            proc_num=proc_num, factors=factors,
            ic_tests_dir=ic_tests_dir,
            available_universe_dir=available_universe_dir,
            exposure_dir=factors_exposure_neutral_dir,
            test_return_dir=test_return_neutral_dir,
            database_structure=database_structure, calendar=calendar,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            neutral_method=neutral_method,
        )
    elif switch in ["ICD"]:
        from ic_tests.ic_tests_factors_delinear import cal_ic_tests_delinear_mp

        cal_ic_tests_delinear_mp(
            proc_num=proc_num,
            pids=[pid], factors_pool_options=factors_pool_options,
            neutral_methods=[neutral_method], test_windows=test_windows, factors_return_lags=[0],
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            ic_tests_delinear_dir=ic_tests_delinear_dir,
            exposure_dir=factors_exposure_delinear_dir,
            return_dir=test_return_neutral_dir,
            calendar_path=calendar_path,
            database_structure=database_structure,
        )
    elif switch in ["ICS"]:
        from ic_tests.ic_tests_cls_summary import CICTestsSummaryRaw

        agent_summary = CICTestsSummaryRaw(
            proc_num=proc_num, ic_tests_dir=ic_tests_dir,
            ic_tests_summary_dir=ic_tests_summary_dir,
            database_structure=database_structure
        )
        agent_summary.get_summary_mp(factors, factors_classification)
        agent_summary.get_cumsum_mp(factors_group)
    elif switch in ["ICNS"]:
        from ic_tests.ic_tests_cls_summary import CICTestsSummaryNeutral

        agent_summary = CICTestsSummaryNeutral(
            neutral_method=neutral_method,
            proc_num=proc_num, ic_tests_dir=ic_tests_dir,
            ic_tests_summary_dir=ic_tests_summary_dir,
            database_structure=database_structure
        )
        agent_summary.get_summary_mp(factors, factors_classification)
        agent_summary.get_cumsum_mp(factors_group)

    elif switch in ["ICDS"]:
        from ic_tests.ic_tests_summary_delinear import cal_ic_tests_delinear_summary_mp

        cal_ic_tests_delinear_summary_mp(
            proc_num=proc_num,
            pids=[pid], factors_pool_options=factors_pool_options,
            neutral_methods=[neutral_method], test_windows=test_windows, factors_return_lags=[0],
            ic_tests_delinear_dir=ic_tests_delinear_dir,
            database_structure=database_structure,
        )
    elif switch in ["ICC"]:
        from ic_tests.ic_tests_comparison import cal_ic_tests_comparison

        cal_ic_tests_comparison(
            factors=factors, neutral_method=neutral_method, exception_list=[],
            ic_tests_dir=ic_tests_dir, top_n=12
        )

    else:
        print(f"... switch = {switch} is not a legal option, please check again.")
