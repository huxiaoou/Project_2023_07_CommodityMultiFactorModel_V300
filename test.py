import itertools

from setup_project import (calendar_path, instrument_info_path, md_by_instru_dir, available_universe_dir,
                           signals_hedge_test_dir, simulations_hedge_test_dir, evaluations_hedge_test_dir,
                           signals_optimized_dir,
                           signals_portfolios_dir, simulations_portfolios_dir, evaluations_portfolios_dir,
                           factors_exposure_dir, factors_exposure_neutral_dir,
                           futures_by_instrument_dir, major_return_db_name)

test_id = ["sig", "simu", "simu2", "opt"][3]

if test_id == "sig":
    from signals.signals_cls import CSignalHedge, CCalendar

    run_mode, bgn_date, stp_date = "O", "20150701", "20230801"
    calendar = CCalendar(calendar_path)
    shared_args = dict(available_universe_dir=available_universe_dir, sig_save_dir=signals_hedge_test_dir, calendar=calendar)
    s = CSignalHedge(uni_prop=0.2, src_factor_dir=factors_exposure_dir, src_factor_id="BASISA126", **shared_args)
    s.main(run_mode, bgn_date, stp_date)

    s = CSignalHedge(uni_prop=0.2, src_factor_dir=factors_exposure_neutral_dir, src_factor_id="BASISA126_WS", **shared_args)
    s.main(run_mode, bgn_date, stp_date)
elif test_id == "simu":
    import os
    from config_project import concerned_instruments_universe
    from simulations.simulation_fun import simulation_single_factor
    from struct_lib.returns_and_exposure import get_lib_struct_available_universe
    from struct_lib.portfolios import get_signal_lib_struct

    major_minor_dir = os.path.join(futures_by_instrument_dir, "major_minor")

    bgn_date, stp_date = "20150701", "20230801"
    cost_rate, init_premium = 0e-4, 1e8
    signal_ids = [
        "BASISA126_UHP02",
        "BASISA126_UHP04",
        "LIQUIDBD010_WS_UHP02",
        "LIQUIDBD010_WS_UHP04",
        "MTM_WS_UHP02",
        "MTM_WS_UHP04", ]
    for signal_id in signal_ids:
        database_structure = {
            "available_universe": get_lib_struct_available_universe(),
            signal_id: get_signal_lib_struct(signal_id)
        }

        simulation_single_factor(
            signal_id=signal_id, hold_period_n=1, start_delay=0,
            calendar_path=calendar_path, instrument_info_path=instrument_info_path,
            md_by_instru_dir=md_by_instru_dir, major_minor_dir=major_minor_dir, available_universe_dir=available_universe_dir,
            sig_dir=signals_hedge_test_dir, dst_dir=simulations_hedge_test_dir,
            database_structure=database_structure, test_universe=concerned_instruments_universe,
            test_bgn_date=bgn_date, test_stp_date=stp_date,
            cost_reservation=0, cost_rate=cost_rate, init_premium=init_premium,
            skip_if_exist=False,
        )
elif test_id == "simu2":
    from simulations.simulation_cls import CSimulation, CCalendar
    from factors.factors_cls_base import CDbByInstrument
    from signals.signals_cls import CSignalReader
    from config_project import concerned_instruments_universe

    # sid, uni_prop = "BASISA126", 0.2
    sids, uni_props = ("BASISA126", "LIQUIDBD010_WS", "MTM_WS"), (0.2, 0.4)
    cost_rate = 0
    test_bgn_date, test_stp_date = "20150701", "20230801"

    for sid, uni_prop in itertools.product(sids, uni_props):
        sig_id = f"{sid}_UHP{int(uni_prop * 10):02d}"
        print(f"\n{sig_id}")
        calendar = CCalendar(calendar_path)
        shared_args = dict(sig_save_dir=signals_hedge_test_dir, calendar=calendar)
        signal = CSignalReader(sig_id=sig_id, **shared_args)
        simu = CSimulation(signal=signal, test_bgn_date=test_bgn_date, test_stp_date=test_stp_date,
                           cost_rate=cost_rate,
                           test_universe=concerned_instruments_universe,
                           manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                           simu_save_dir=simulations_hedge_test_dir,
                           calendar=calendar)
        simu.main()
elif test_id == "opt":
    from signals.signals_cls_optimizer import CSignalOptimizerMinUty, CSignalOptimizerMinUtyCon, CSignalOptimizerMinNegSharpe, CCalendarMonthly
    from signals.signals_cls_portfolio import CSignalCombineFromOtherSignalsWithDynWeight, CSignalCombineFromOtherSignalsWithFixWeight
    from factors.factors_cls_base import CDbByInstrument
    from simulations.simulation_cls import CSimulation
    from simulations.evaluations_cls import CEvaluationPortfolio
    from config_portfolio import cost_rate_portfolios, performance_indicators, size_raw, size_neu, src_signal_ids_raw, src_signal_ids_neu
    from config_project import concerned_instruments_universe
    import sys

    trn_win, lbd, fee_adj_rate = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
    min_model_days = int(trn_win * 21 * 0.9)
    calendar = CCalendarMonthly(calendar_path)
    bgn_date, stp_date = "20150701", "20230801"

    # --- RAW EQL
    signals = CSignalCombineFromOtherSignalsWithFixWeight(src_signal_weight={_: 1 / size_raw for _ in src_signal_ids_raw},
                                                          src_signal_ids=src_signal_ids_raw, src_signal_dir=signals_hedge_test_dir, sig_id="raw_fix",
                                                          sig_save_dir=signals_portfolios_dir, calendar=calendar)
    signals.main("O", bgn_date, stp_date)
    simu = CSimulation(signal=signals, test_bgn_date=bgn_date, test_stp_date=stp_date,
                       cost_rate=cost_rate_portfolios * fee_adj_rate, test_universe=concerned_instruments_universe,
                       manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                       simu_save_dir=simulations_portfolios_dir,
                       calendar=calendar)
    simu.main()

    # --- NEW EQL
    signals = CSignalCombineFromOtherSignalsWithFixWeight(src_signal_weight={_: 1 / size_neu for _ in src_signal_ids_neu},
                                                          src_signal_ids=src_signal_ids_neu, src_signal_dir=signals_hedge_test_dir, sig_id="neu_fix",
                                                          sig_save_dir=signals_portfolios_dir, calendar=calendar)
    signals.main("O", bgn_date, stp_date)
    simu = CSimulation(signal=signals, test_bgn_date=bgn_date, test_stp_date=stp_date,
                       cost_rate=cost_rate_portfolios * fee_adj_rate, test_universe=concerned_instruments_universe,
                       manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                       simu_save_dir=simulations_portfolios_dir,
                       calendar=calendar)
    simu.main()

    # --- RAW OPT
    optimizer = CSignalOptimizerMinUtyCon(save_id="raw_min_uty_con", src_signal_ids=src_signal_ids_raw,
                                          weight_bounds=(1 / size_raw / 2, 2 / size_raw), total_pos_lim=(0, 1), maxiter=10000,
                                          trn_win=trn_win, min_model_days=min_model_days, lbd=lbd,
                                          simu_test_dir=simulations_hedge_test_dir, optimized_dir=signals_optimized_dir,
                                          calendar=calendar)
    optimizer.main("O", bgn_date, stp_date)
    signal_weight_df = optimizer.get_signal_weight(bgn_date, stp_date)
    signals = CSignalCombineFromOtherSignalsWithDynWeight(src_signal_weight=signal_weight_df,
                                                          src_signal_ids=src_signal_ids_raw, src_signal_dir=signals_hedge_test_dir, sig_id="raw_min_uty_con",
                                                          sig_save_dir=signals_portfolios_dir, calendar=calendar)
    signals.main("O", bgn_date, stp_date)
    simu = CSimulation(signal=signals, test_bgn_date=bgn_date, test_stp_date=stp_date,
                       cost_rate=cost_rate_portfolios * fee_adj_rate, test_universe=concerned_instruments_universe,
                       manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                       simu_save_dir=simulations_portfolios_dir,
                       calendar=calendar)
    simu.main()

    # --- NEU
    optimizer = CSignalOptimizerMinUtyCon(save_id="neu_min_uty_con", src_signal_ids=src_signal_ids_neu,
                                          weight_bounds=(1 / size_neu / 2, 2 / size_neu), total_pos_lim=(0, 1), maxiter=10000,
                                          trn_win=trn_win, min_model_days=min_model_days, lbd=lbd,
                                          simu_test_dir=simulations_hedge_test_dir, optimized_dir=signals_optimized_dir,
                                          calendar=calendar)
    optimizer.main("O", bgn_date, stp_date)
    signal_weight_df = optimizer.get_signal_weight(bgn_date, stp_date)
    signals = CSignalCombineFromOtherSignalsWithDynWeight(src_signal_weight=signal_weight_df,
                                                          src_signal_ids=src_signal_ids_neu, src_signal_dir=signals_hedge_test_dir, sig_id="neu_min_uty_con",
                                                          sig_save_dir=signals_portfolios_dir, calendar=calendar)
    signals.main("O", bgn_date, stp_date)
    simu = CSimulation(signal=signals, test_bgn_date=bgn_date, test_stp_date=stp_date,
                       cost_rate=cost_rate_portfolios * fee_adj_rate, test_universe=concerned_instruments_universe,
                       manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                       simu_save_dir=simulations_portfolios_dir,
                       calendar=calendar)
    simu.main()

    # --- RAW on-con
    optimizer = CSignalOptimizerMinUty(save_id="raw_min_uty", src_signal_ids=src_signal_ids_raw,
                                       trn_win=trn_win, min_model_days=min_model_days, lbd=lbd,
                                       simu_test_dir=simulations_hedge_test_dir, optimized_dir=signals_optimized_dir,
                                       calendar=calendar)
    optimizer.main("O", bgn_date, stp_date)
    signal_weight_df = optimizer.get_signal_weight(bgn_date, stp_date)
    signals = CSignalCombineFromOtherSignalsWithDynWeight(src_signal_weight=signal_weight_df,
                                                          src_signal_ids=src_signal_ids_raw, src_signal_dir=signals_hedge_test_dir, sig_id="raw_min_uty",
                                                          sig_save_dir=signals_portfolios_dir, calendar=calendar)
    signals.main("O", bgn_date, stp_date)
    simu = CSimulation(signal=signals, test_bgn_date=bgn_date, test_stp_date=stp_date,
                       cost_rate=cost_rate_portfolios * fee_adj_rate, test_universe=concerned_instruments_universe,
                       manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                       simu_save_dir=simulations_portfolios_dir,
                       calendar=calendar)
    simu.main()

    # --- NEU on-con
    optimizer = CSignalOptimizerMinUty(save_id="neu_min_uty", src_signal_ids=src_signal_ids_neu,
                                       trn_win=trn_win, min_model_days=int(trn_win * 21 * 0.9), lbd=lbd,
                                       simu_test_dir=simulations_hedge_test_dir, optimized_dir=signals_optimized_dir,
                                       calendar=calendar)
    optimizer.main("O", bgn_date, stp_date)
    signal_weight_df = optimizer.get_signal_weight(bgn_date, stp_date)
    signals = CSignalCombineFromOtherSignalsWithDynWeight(src_signal_weight=signal_weight_df,
                                                          src_signal_ids=src_signal_ids_neu, src_signal_dir=signals_hedge_test_dir, sig_id="neu_min_uty",
                                                          sig_save_dir=signals_portfolios_dir, calendar=calendar)
    signals.main("O", bgn_date, stp_date)
    simu = CSimulation(signal=signals, test_bgn_date=bgn_date, test_stp_date=stp_date,
                       cost_rate=cost_rate_portfolios * fee_adj_rate, test_universe=concerned_instruments_universe,
                       manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                       simu_save_dir=simulations_portfolios_dir,
                       calendar=calendar)
    simu.main()

    # --- NEU sharpe
    optimizer = CSignalOptimizerMinNegSharpe(save_id="neu_max_sharpe", src_signal_ids=src_signal_ids_neu,
                                             weight_bounds=(1 / size_neu / 2, 2 / size_neu), maxiter=10000,
                                             trn_win=trn_win, min_model_days=min_model_days,
                                             simu_test_dir=simulations_hedge_test_dir, optimized_dir=signals_optimized_dir,
                                             calendar=calendar)
    optimizer.main("O", bgn_date, stp_date)
    signal_weight_df = optimizer.get_signal_weight(bgn_date, stp_date)
    signals = CSignalCombineFromOtherSignalsWithDynWeight(src_signal_weight=signal_weight_df,
                                                          src_signal_ids=src_signal_ids_neu, src_signal_dir=signals_hedge_test_dir, sig_id="neu_max_sharpe",
                                                          sig_save_dir=signals_portfolios_dir, calendar=calendar)
    signals.main("O", bgn_date, stp_date)
    simu = CSimulation(signal=signals, test_bgn_date=bgn_date, test_stp_date=stp_date,
                       cost_rate=cost_rate_portfolios * fee_adj_rate, test_universe=concerned_instruments_universe,
                       manager_major_return=CDbByInstrument(futures_by_instrument_dir, major_return_db_name),
                       simu_save_dir=simulations_portfolios_dir,
                       calendar=calendar)
    simu.main()

    # --- evaluation
    evaluator = CEvaluationPortfolio(
        eval_id="test", simu_ids=["raw_fix", "neu_fix", "raw_min_uty_con", "neu_min_uty_con", "raw_min_uty", "neu_min_uty", "neu_max_sharpe"],
        indicators=performance_indicators, simu_save_dir=simulations_portfolios_dir,
        eval_save_dir=evaluations_portfolios_dir, annual_risk_free_rate=0,
    )
    evaluator.main(True)
