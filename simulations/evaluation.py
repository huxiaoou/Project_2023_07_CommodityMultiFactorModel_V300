import os
import datetime as dt
import pandas as pd
import multiprocessing as mp
from skyrim.riften import CNAV
from skyrim.winterhold import plot_lines
from skyrim.whiterun import df_to_md_files


def evaluation_signal(
        signal_id: str,
        hold_period_n_list: list,
        bgn_date: str, stp_date: str,
        risk_free_rate: float,
        src_simulations_dir: str,
        dst_evaluations_dir: str,
        top_n: int,
        verbose: bool,
):
    pd.set_option("display.width", 0)

    index_cols = ["HPN"]
    latex_cols = ["持有期收益", "年化收益", "夏普比率", "最大回撤", "最大回撤时点", "最长回撤期", "最长恢复期", "卡玛比率"]
    md_cols = ["持有期收益", "年化收益", "夏普比率", "最大回撤", "最大回撤时点", "卡玛比率"]

    aver_nav_summary_data = []
    for hold_period_n in hold_period_n_list:
        comb_id = "{}.HPN{:03d}".format(signal_id, hold_period_n)

        # for each delay in hold period n
        port_id_list = [comb_id + ".D{:02d}".format(delay_id) for delay_id in range(hold_period_n)]
        nav_data = {}
        for port_id in port_id_list:
            simu_nav_file = "{}.nav.daily.csv.gz".format(port_id)
            simu_nav_path = os.path.join(src_simulations_dir, port_id, simu_nav_file)
            simu_nav_df = pd.read_csv(simu_nav_path, dtype={"trade_date": str}).set_index("trade_date")
            nav_data[port_id] = simu_nav_df["navps"]

        # average nav for all the delay
        nav_df = pd.DataFrame(nav_data).fillna(1)
        filter_date = (nav_df.index >= bgn_date) & (nav_df.index < stp_date)
        nav_df = nav_df.loc[filter_date]
        nav_df["AVER"] = nav_df[port_id_list].mean(axis=1)
        nav_file = "nav.{}.csv.gz".format(comb_id)
        nav_path = os.path.join(dst_evaluations_dir, "by_comb_id", nav_file)
        nav_df.to_csv(nav_path, float_format="%.6f")

        # get nav summary
        p_nav = CNAV(t_raw_nav_srs=nav_df["AVER"], t_annual_rf_rate=risk_free_rate)
        p_nav.cal_all_indicators(t_method="compound")
        d = p_nav.to_dict(t_type="chs")
        d.update({
            "HPN": hold_period_n,
        })
        aver_nav_summary_data.append(d)

        # plot AVER nav
        plot_lines(
            t_plot_df=nav_df[["AVER"]],
            t_vlines_index=["20230306"],
            t_fig_name="nav.{}".format(comb_id),
            t_save_dir=os.path.join(dst_evaluations_dir, "by_comb_id")
        )

    aver_nav_summary_df = pd.DataFrame(aver_nav_summary_data).sort_values(by=index_cols, ascending=True).set_index(index_cols)
    aver_nav_summary_file = "summary.{}.aver.csv".format(signal_id)
    aver_nav_summary_path = os.path.join(dst_evaluations_dir, aver_nav_summary_file)
    aver_nav_summary_df.to_csv(aver_nav_summary_path, float_format="%.2f")
    aver_nav_summary_df[latex_cols].to_csv(aver_nav_summary_path.replace(".csv", ".latex.csv"), float_format="%.2f")
    df_to_md_files(aver_nav_summary_df[md_cols].reset_index(), aver_nav_summary_path.replace(".csv", ".md"))
    aver_nav_summary_df["年化收益"] = aver_nav_summary_df["年化收益"].astype(float)
    aver_nav_summary_df["夏普比率"] = aver_nav_summary_df["夏普比率"].astype(float)

    # plot top n
    for evaluation_idx in ["夏普比率"]:
        # load summary
        sorted_summary_df = aver_nav_summary_df.sort_values(by=evaluation_idx, ascending=False).head(top_n)
        sorted_summary_file = "summary.{}.aver.top{:02d}.{}.csv".format(signal_id, top_n, evaluation_idx)
        sorted_summary_path = os.path.join(dst_evaluations_dir, sorted_summary_file)
        sorted_summary_df.to_csv(sorted_summary_path, float_format="%.2f")

        # load nav data
        top_nav_data = {}
        for hold_period_n in sorted_summary_df.index:
            signal_id = signal_id
            comb_id = "{}.HPN{:03d}".format(signal_id, hold_period_n)
            nav_file = "nav.{}.csv.gz".format(comb_id)
            nav_path = os.path.join(dst_evaluations_dir, "by_comb_id", nav_file)
            nav_df = pd.read_csv(nav_path, dtype={"trade_date": str}).set_index("trade_date")
            top_nav_data[comb_id] = nav_df["AVER"]
        top_nav_df = pd.DataFrame(top_nav_data)
        plot_lines(
            t_plot_df=top_nav_df,
            t_vlines_index=["20230306"],
            t_fig_name="nav.{}.aver.top{:02d}.{}".format(signal_id, top_n, evaluation_idx),
            t_save_dir=dst_evaluations_dir)

        if verbose:
            print("=" * 120)
            print(evaluation_idx + "-" + signal_id)
            print("-" * 120)
            print(sorted_summary_df)
            print("=" * 120)
            print("\n")
    return 0


def cal_evaluation_signals_mp(proc_num: int, signal_ids: list[str], **kwargs):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for signal_id in signal_ids:
        pool.apply_async(evaluation_signal, args=(signal_id,), kwds=kwargs)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
