import os
import pandas as pd
from skyrim.riften import CNAV
from skyrim.winterhold import plot_lines
from skyrim.whiterun import df_to_md_files


def evaluate_signal_by_year(signal_id: str, hold_period_n: int,
                            risk_free_rate: float,
                            evaluations_allocation_dir: str, by_year_dir: str):
    pd.set_option("display.width", 0)
    comb_id = "{}.HPN{:03d}".format(signal_id, hold_period_n)

    # core settings
    index_cols = "年"
    latex_cols = ["天数", "持有期收益", "年化收益", "夏普比率", "最大回撤", "最大回撤时点", "最长回撤期", "最长恢复期", "卡玛比率"]
    md_cols = ["天数", "持有期收益", "年化收益", "夏普比率", "最大回撤", "最大回撤时点", "卡玛比率"]

    # load nav file
    nav_file = "nav.{}.csv.gz".format(comb_id)
    nav_path = os.path.join(evaluations_allocation_dir, "by_comb_id", nav_file)
    nav_df = pd.read_csv(nav_path, dtype={"trade_date": str}).set_index("trade_date")
    nav_df["trade_year"] = nav_df.index.map(lambda z: z[0:4])
    # print(nav_df.tail(20))

    # by year
    by_year_nav_summary_data = []
    for trade_year, trade_year_df in nav_df.groupby(by="trade_year"):
        # get nav summary
        p_nav = CNAV(t_raw_nav_srs=trade_year_df["AVER"], t_annual_rf_rate=risk_free_rate)
        p_nav.cal_all_indicators(t_method="compound")
        d = p_nav.to_dict(t_type="chs")
        d.update({
            "年": trade_year,
            "天数": len(trade_year_df),
        })
        by_year_nav_summary_data.append(d)
        plot_lines(
            t_plot_df=trade_year_df[["AVER"]] / trade_year_df["AVER"].iloc[0],
            t_vlines_index=None,
            t_fig_name="nav.{}.Y{}".format(comb_id, trade_year),
            t_save_dir=by_year_dir
        )

    aver_nav_summary_df = pd.DataFrame(by_year_nav_summary_data).sort_values(by=index_cols, ascending=True).set_index(index_cols)
    aver_nav_summary_file = "summary.{}.by_year.csv".format(comb_id)
    aver_nav_summary_path = os.path.join(by_year_dir, aver_nav_summary_file)
    aver_nav_summary_df.to_csv(aver_nav_summary_path, float_format="%.2f")
    aver_nav_summary_df[latex_cols].to_csv(aver_nav_summary_path.replace(".csv", ".latex.csv"), float_format="%.2f")
    df_to_md_files(aver_nav_summary_df[md_cols].reset_index(), aver_nav_summary_path.replace(".csv", ".md"))
    print("=" * 120)
    print("AVER NAV SUMMARY BY YEAR", signal_id, hold_period_n)
    print("-" * 120)
    print(aver_nav_summary_df)
    print("-" * 120 + "\n")
    return 0


def plot_signals_nav_by_year(output_id: str, signal_id_and_hpn_options: list[(str, int)], evaluations_allocation_dir: str, by_year_dir: str):
    multiple_nav_data = {}
    for allocation_id, hold_period_n in signal_id_and_hpn_options:
        comb_id = "{}.HPN{:03d}".format(allocation_id, hold_period_n)
        nav_file = "nav.{}.csv.gz".format(comb_id)
        nav_path = os.path.join(evaluations_allocation_dir, "by_comb_id", nav_file)
        nav_df = pd.read_csv(nav_path, dtype={"trade_date": str}).set_index("trade_date")
        multiple_nav_data[comb_id] = nav_df["AVER"]
    multiple_nav_df = pd.DataFrame(multiple_nav_data)
    norm_plot_df = multiple_nav_df / multiple_nav_df.iloc[0]
    plot_lines(
        t_plot_df=norm_plot_df,
        t_fig_name="nav.{}".format(output_id),
        t_colormap="jet",
        t_save_dir=by_year_dir
    )
    multiple_nav_df["trade_year"] = multiple_nav_df.index.map(lambda z: z[0:4])
    for trade_year, trade_year_df in multiple_nav_df.groupby(by="trade_year"):
        plot_df = trade_year_df.drop(labels="trade_year", axis=1)
        norm_plot_df = plot_df / plot_df.iloc[0]
        plot_lines(
            t_plot_df=norm_plot_df,
            # t_vlines_index=["20221014"] if trade_year == "2022" else None,
            t_fig_name="nav.{}.Y{}".format(output_id, trade_year),
            t_colormap="jet",
            t_save_dir=by_year_dir
        )
        print("... multiple nav of {:>24s} of year {} plot".format(output_id, trade_year))
    return 0
