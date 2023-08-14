from setup_project import (calendar_path, instrument_info_path, md_by_instru_dir, available_universe_dir,
                           signals_hedge_test_dir, simulations_hedge_test_dir, factors_exposure_dir, factors_exposure_neutral_dir)

test_id = ["sig", "simu"][1]

if test_id == "sig":
    from signals.signals_cls import CSignalHedge, CCalendar

    run_mode, bgn_date, stp_date = "O", "20150701", "20230801"
    calendar = CCalendar(calendar_path)
    shared_args = dict(available_universe_dir=available_universe_dir, sig_save_dir=signals_hedge_test_dir, calendar=calendar)
    s = CSignalHedge(uni_prop=0.2,
                     src_factor_dir=factors_exposure_dir, src_factor_id="BASISA126",
                     **shared_args)

    s.main(run_mode, bgn_date, stp_date)

    s = CSignalHedge(uni_prop=0.2,
                     src_factor_dir=factors_exposure_neutral_dir, src_factor_id="BASISA126_WS",
                     **shared_args)
    s.main(run_mode, bgn_date, stp_date)
elif test_id == "simu":
    import os
    from setup_project import futures_by_instrument_dir
    from config_project import concerned_instruments_universe
    from simulations.simulation_fun import simulation_single_factor
    from struct_lib.returns_and_exposure import get_lib_struct_available_universe
    from struct_lib.portfolios import get_signal_lib_struct

    major_minor_dir = os.path.join(futures_by_instrument_dir, "major_minor")

    bgn_date, stp_date = "20150701", "20230801"
    # signal_id = "BASISA126_UHP04"
    # signal_id = "LIQUIDBD010_WS_UHP02"
    # signal_id = "LIQUIDBD010_WS_UHP04"
    # signal_id = "MTM_WS_UHP02"
    signal_id = "MTM_WS_UHP04"
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
        cost_reservation=0, cost_rate=3e-4, init_premium=1e8,
        skip_if_exist=False,
    )
