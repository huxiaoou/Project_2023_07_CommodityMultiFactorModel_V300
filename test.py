from signals.signals_cls import CSignalHedge, CCalendar
from setup_project import calendar_path, available_universe_dir, signals_hedge_test_dir, factors_exposure_dir, factors_exposure_neutral_dir

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
print(s.sig_id)
