from signals.signals_cls import CSignalHedge, CCalendar
from setup_project import calendar_path

calendar = CCalendar(calendar_path)

s = CSignalHedge(prop=0.2, sig_save_dir=".", sig_id="TEST", src_factor_dir=".", src_factor_id="BASIS147", calendar=calendar)
print(s.sig_id)