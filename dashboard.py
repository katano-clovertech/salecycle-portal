"""
SaleCycle レポートポータル
実行: streamlit run dashboard.py
"""

import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta, date

CSV_PATH = os.path.join(os.path.dirname(__file__), "salecycle_daily_report.csv")
COL_DATE   = "日付"
COL_CLIENT = "クライアント"
COL_DASH   = "ダッシュボード種別"
COL_SENDS  = "送付件数"
COL_OPENS  = "開封数"
COL_CLICKS = "クリック数"
COL_CVS    = "コンバージョン数"
COL_REV    = "コンバージョン金額"

DASH_ORDER  = ["Basket", "Browse", "Display"]
DASH_COLORS = {"Basket": "#4A90D9", "Browse": "#27AE60", "Display": "#E67E22"}

st.set_page_config(
    page_title="SaleCycle レポートポータル",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
.block-label {
    font-size:1.1rem; font-weight:700; padding:6px 14px;
    border-radius:6px; color:white; margin-bottom:8px; display:inline-block;
}
.basket-label  { background:#4A90D9; }
.browse-label  { background:#27AE60; }
.display-label { background:#E67E22; }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def load_data():
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    df[COL_DATE]   = pd.to_datetime(df[COL_DATE], format="mixed")
    df[COL_SENDS]  = pd.to_numeric(df[COL_SENDS],  errors="coerce").fillna(0)
    df[COL_OPENS]  = pd.to_numeric(df[COL_OPENS],  errors="coerce").fillna(0)
    df[COL_CLICKS] = pd.to_numeric(df[COL_CLICKS], errors="coerce").fillna(0)
    df[COL_CVS]    = pd.to_numeric(df[COL_CVS],    errors="coerce").fillna(0)
    if COL_REV in df.columns:
        df[COL_REV] = pd.to_numeric(df[COL_REV], errors="coerce").fillna(0)
    else:
        df[COL_REV] = 0
    df["年月"]      = df[COL_DATE].dt.to_period("M").astype(str)
    df["開封率"]    = df.apply(lambda r: r[COL_OPENS]  / r[COL_SENDS] * 100 if r[COL_SENDS] > 0 else 0, axis=1)
    df["クリック率"]= df.apply(lambda r: r[COL_CLICKS] / r[COL_SENDS] * 100 if r[COL_SENDS] > 0 else 0, axis=1)
    df["CVR"]       = df.apply(lambda r: r[COL_CVS]    / r[COL_SENDS] * 100 if r[COL_SENDS] > 0 else 0, axis=1)
    return df


df_all = load_data()

st.title("📊 SaleCycle レポートポータル")
st.caption(f"データ取得元: {CSV_PATH}")

if df_all.empty:
    st.error("データが見つかりません。salecycle_daily_report.csv を確認してください。")
    st.stop()

all_clients = sorted(df_all[COL_CLIENT].unique().tolist())
all_dates   = sorted(df_all[COL_DATE].dt.date.unique())

tab1, tab2, tab3, tab4 = st.tabs(["📤 送付件数一覧", "📅 期間成果", "📆 月別成果", "📋 週次レポート"])


# ════════════════════════════════════
#  TAB 1: 送付件数一覧
# ════════════════════════════════════
with tab1:
    st.subheader("送付件数一覧")
    st.caption("Basket / Browse / Display ブロックごとに、クライアント × 日付の送付件数マトリクス")

    search_t1 = st.text_input("クライアント検索", placeholder="例: Oisix", key="t1_search")

    df_t1 = df_all.copy()
    if search_t1:
        df_t1 = df_t1[df_t1[COL_CLIENT].str.contains(search_t1, case=False, na=False)]

    # 日付列（新しい順）
    date_cols = sorted(df_t1[COL_DATE].dt.strftime("%Y-%m-%d").unique(), reverse=True)

    for dash in DASH_ORDER:
        df_dash_t1 = df_t1[df_t1[COL_DASH] == dash]
        if df_dash_t1.empty:
            continue

        label_cls = dash.lower() + "-label"
        st.markdown(f'<span class="block-label {label_cls}">{dash}</span>', unsafe_allow_html=True)

        # クライアント × 日付 のピボット（送付件数のみ）
        pivot = df_dash_t1.pivot_table(
            index=COL_CLIENT, columns=COL_DATE, values=COL_SENDS, aggfunc="sum"
        ).fillna(0).astype(int)
        pivot.columns = [c.strftime("%m/%d") for c in pivot.columns]
        # 最新日の送付件数順でソート・新しい日付を左に並べる
        latest_col = pivot.columns[-1]
        pivot = pivot.sort_values(latest_col, ascending=False)
        pivot = pivot[pivot.columns[::-1]]  # 日付を新しい順（左）に並べる

        col_cfg_t1 = {c: st.column_config.NumberColumn(c, format="%d") for c in pivot.columns}
        st.dataframe(pivot, use_container_width=True, hide_index=False, column_config=col_cfg_t1)

        # 過去1か月の送付数推移（折れ線グラフ）
        cutoff = df_dash_t1[COL_DATE].max() - pd.Timedelta(days=30)
        df_trend = df_dash_t1[df_dash_t1[COL_DATE] >= cutoff].groupby([COL_DATE, COL_CLIENT])[COL_SENDS].sum().reset_index()
        if not df_trend.empty:
            all_clients = sorted(df_trend[COL_CLIENT].unique().tolist())
            top_clients = df_trend.groupby(COL_CLIENT)[COL_SENDS].sum().nlargest(10).index.tolist()
            selected = st.multiselect(
                f"{dash} グラフ表示クライアント",
                options=all_clients,
                default=top_clients,
                key=f"t1_chart_{dash}",
            )
            if selected:
                df_trend_top = df_trend[df_trend[COL_CLIENT].isin(selected)]
                fig_t1 = px.line(
                    df_trend_top, x=COL_DATE, y=COL_SENDS, color=COL_CLIENT,
                    title=f"{dash} - 過去1ヶ月 送付数推移",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_t1.update_layout(
                    height=350, margin=dict(t=40, b=20, l=20, r=20),
                    legend=dict(orientation="h", y=-0.25),
                    xaxis_title="", yaxis_title="送付数",
                )
                st.plotly_chart(fig_t1, use_container_width=True)
        st.divider()


# ════════════════════════════════════
#  TAB 2: 期間成果
# ════════════════════════════════════
with tab2:
    st.subheader("期間成果")

    cf1, cf2 = st.columns([3, 2])
    with cf1:
        min_d = min(all_dates); max_d = max(all_dates)
        default_start = max(min_d, max_d - timedelta(days=6))
        date_range = st.date_input(
            "期間指定",
            value=(default_start, max_d),
            min_value=min_d, max_value=max_d,
            key="t2_range"
        )
    with cf2:
        granularity = st.selectbox("集計粒度", ["日", "週", "月"], key="t2_gran")

    sel_clients_t2 = st.multiselect(
        "クライアント（複数選択可・未選択で全件）",
        all_clients, key="t2_clients"
    )

    # 期間を確定
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        d_start, d_end = date_range[0], date_range[1]
    else:
        d_start = d_end = date_range if not isinstance(date_range, (list, tuple)) else date_range[0]

    df_t2 = df_all[
        (df_all[COL_DATE].dt.date >= d_start) &
        (df_all[COL_DATE].dt.date <= d_end)
    ].copy()
    if sel_clients_t2:
        df_t2 = df_t2[df_t2[COL_CLIENT].isin(sel_clients_t2)]

    gran_label = {"日": "D", "週": "W", "月": "M"}
    freq = gran_label[granularity]
    if freq == "D":
        df_t2["期間"] = df_t2[COL_DATE].dt.strftime("%Y-%m-%d")
    elif freq == "W":
        df_t2["期間"] = df_t2[COL_DATE].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
    else:
        df_t2["期間"] = df_t2[COL_DATE].dt.to_period("M").astype(str)

    st.divider()

    # Basket / Browse / Display ブロック
    for dash in DASH_ORDER:
        df_dash = df_t2[df_t2[COL_DASH] == dash]
        if df_dash.empty:
            continue

        label_cls = dash.lower() + "-label"
        st.markdown(f'<span class="block-label {label_cls}">{dash}</span>', unsafe_allow_html=True)

        # ── 表（全幅）──
        tbl = df_dash.groupby(COL_CLIENT).agg(
            送付件数=(COL_SENDS, "sum"),
            開封数=(COL_OPENS, "sum"),
            クリック数=(COL_CLICKS, "sum"),
            CV数=(COL_CVS, "sum"),
            コンバージョン金額=(COL_REV, "sum"),
        ).reset_index().sort_values("送付件数", ascending=False)

        if dash != "Display":
            tbl["開封率"] = tbl.apply(
                lambda r: f"{r['開封数']/r['送付件数']*100:.1f}%" if r["送付件数"] > 0 else "-", axis=1)
        tbl["クリック率"] = tbl.apply(
            lambda r: f"{r['クリック数']/r['送付件数']*100:.1f}%" if r["送付件数"] > 0 else "-", axis=1)
        tbl["CVR"] = tbl.apply(
            lambda r: f"{r['CV数']/r['送付件数']*100:.2f}%" if r["送付件数"] > 0 else "-", axis=1)

        show_cols = [COL_CLIENT, "送付件数"]
        if dash != "Display":
            show_cols += ["開封数", "開封率"]
        show_cols += ["クリック数", "クリック率", "CV数", "CVR", "コンバージョン金額"]

        st.dataframe(
            tbl[show_cols].reset_index(drop=True),
            use_container_width=True, hide_index=True,
            column_config={
                "送付件数":       st.column_config.NumberColumn("送付",          format="%d"),
                "開封数":         st.column_config.NumberColumn("開封",          format="%d"),
                "クリック数":     st.column_config.NumberColumn("Click",         format="%d"),
                "CV数":           st.column_config.NumberColumn("CV",            format="%d"),
                "コンバージョン金額": st.column_config.NumberColumn("CV金額(JPY)", format="%d"),
            }
        )

        # ── グラフ（表の下・全幅）──
        agg_g = df_dash.groupby(["期間", COL_CLIENT]).agg(
            **{"送付件数": (COL_SENDS, "sum")}
        ).reset_index()
        top_clients = agg_g.groupby(COL_CLIENT)["送付件数"].sum().nlargest(10).index.tolist()
        agg_g_top = agg_g[agg_g[COL_CLIENT].isin(top_clients)]
        fig = px.bar(
            agg_g_top, x="期間", y="送付件数", color=COL_CLIENT,
            barmode="stack", title=f"{dash} - 送付件数推移（上位10社）",
            color_discrete_sequence=px.colors.qualitative.Set2, height=300
        )
        fig.update_layout(xaxis_title="", legend_title="クライアント",
                          legend=dict(orientation="h", y=-0.3))
        st.plotly_chart(fig, use_container_width=True)
        st.divider()


# ════════════════════════════════════
#  TAB 3: 月別成果
# ════════════════════════════════════
with tab3:
    st.subheader("月別成果")

    months = sorted(df_all["年月"].unique(), reverse=True)
    cm1, cm2 = st.columns([2, 3])
    with cm1:
        sel_month = st.selectbox("対象月", months, index=0, key="t3_month")
    with cm2:
        sel_clients_t3 = st.multiselect(
            "クライアント（複数選択可・未選択で全件）",
            all_clients, key="t3_clients"
        )

    df_m = df_all[df_all["年月"] == sel_month].copy()
    if sel_clients_t3:
        df_m = df_m[df_m[COL_CLIENT].isin(sel_clients_t3)]

    prev_month_str = (pd.Period(sel_month, "M") - 1).strftime("%Y-%m")
    df_pm = df_all[df_all["年月"] == prev_month_str].copy()
    if sel_clients_t3:
        df_pm = df_pm[df_pm[COL_CLIENT].isin(sel_clients_t3)]

    # KPI 前月比
    def delta(cur, prev):
        return f"{(cur - prev) / prev * 100:+.1f}%" if prev > 0 else None

    km1, km2, km3, km4, km5 = st.columns(5)
    cs = int(df_m[COL_SENDS].sum());  ps = int(df_pm[COL_SENDS].sum())
    co = int(df_m[COL_OPENS].sum());  po = int(df_pm[COL_OPENS].sum())
    cc = int(df_m[COL_CLICKS].sum()); pc = int(df_pm[COL_CLICKS].sum())
    cv = int(df_m[COL_CVS].sum());    pv = int(df_pm[COL_CVS].sum())
    cr = int(df_m[COL_REV].sum());    pr = int(df_pm[COL_REV].sum())
    km1.metric("合計送付件数",   f"{cs:,}", delta(cs, ps))
    km2.metric("合計開封数",     f"{co:,}", delta(co, po))
    km3.metric("合計クリック数", f"{cc:,}", delta(cc, pc))
    km4.metric("合計CV数",       f"{cv:,}", delta(cv, pv))
    km5.metric("合計CV金額",     f"¥{cr:,}", delta(cr, pr))
    st.divider()

    # Basket / Browse / Display ブロック
    for dash in DASH_ORDER:
        df_dash_m = df_m[df_m[COL_DASH] == dash]
        if df_dash_m.empty:
            continue

        label_cls = dash.lower() + "-label"
        st.markdown(f'<span class="block-label {label_cls}">{dash}</span>', unsafe_allow_html=True)

        agg_m = df_dash_m.groupby(COL_CLIENT).agg(
            送付件数=(COL_SENDS, "sum"),
            開封数=(COL_OPENS, "sum"),
            クリック数=(COL_CLICKS, "sum"),
            CV数=(COL_CVS, "sum"),
            コンバージョン金額=(COL_REV, "sum"),
        ).reset_index().sort_values("送付件数", ascending=False)

        # 前月
        agg_pm = (
            df_pm[df_pm[COL_DASH] == dash].groupby(COL_CLIENT).agg(前月送付=(COL_SENDS, "sum")).reset_index()
            if not df_pm.empty else pd.DataFrame(columns=[COL_CLIENT, "前月送付"])
        )
        agg_m = pd.merge(agg_m, agg_pm, on=COL_CLIENT, how="left")
        agg_m["前月送付"] = agg_m["前月送付"].fillna(0)
        agg_m["前月比"] = agg_m.apply(
            lambda r: f"{(r['送付件数'] - r['前月送付']) / r['前月送付'] * 100:+.1f}%"
            if r["前月送付"] > 0 else "-", axis=1
        )
        if dash != "Display":
            agg_m["開封率"] = agg_m.apply(
                lambda r: f"{r['開封数']/r['送付件数']*100:.1f}%" if r["送付件数"] > 0 else "-", axis=1)
        agg_m["クリック率"] = agg_m.apply(
            lambda r: f"{r['クリック数']/r['送付件数']*100:.1f}%" if r["送付件数"] > 0 else "-", axis=1)
        agg_m["CVR"] = agg_m.apply(
            lambda r: f"{r['CV数']/r['送付件数']*100:.2f}%" if r["送付件数"] > 0 else "-", axis=1)

        # ── 表（全幅）──
        show_cols_m = [COL_CLIENT, "送付件数", "前月送付", "前月比"]
        if dash != "Display":
            show_cols_m += ["開封数", "開封率"]
        show_cols_m += ["クリック数", "クリック率", "CV数", "CVR", "コンバージョン金額"]
        st.dataframe(
            agg_m[show_cols_m].reset_index(drop=True),
            use_container_width=True, hide_index=True,
            column_config={
                "送付件数":       st.column_config.NumberColumn("今月送付",      format="%d"),
                "前月送付":       st.column_config.NumberColumn("前月送付",      format="%d"),
                "開封数":         st.column_config.NumberColumn("開封",          format="%d"),
                "クリック数":     st.column_config.NumberColumn("Click",         format="%d"),
                "CV数":           st.column_config.NumberColumn("CV",            format="%d"),
                "コンバージョン金額": st.column_config.NumberColumn("CV金額(JPY)", format="%d"),
            }
        )

        # ── グラフ（表の下・全幅）──
        top15 = agg_m.nlargest(15, "送付件数")
        fig_m = go.Figure()
        fig_m.add_trace(go.Bar(
            name=prev_month_str, x=top15[COL_CLIENT], y=top15["前月送付"],
            marker_color="#aec6cf"
        ))
        fig_m.add_trace(go.Bar(
            name=sel_month, x=top15[COL_CLIENT], y=top15["送付件数"],
            marker_color=DASH_COLORS[dash]
        ))
        fig_m.update_layout(
            barmode="group",
            title=f"{dash} 送付件数 前月比（上位15社）",
            xaxis_tickangle=-35, height=320,
            legend=dict(orientation="h", y=1.05)
        )
        st.plotly_chart(fig_m, use_container_width=True)
        st.divider()

    # 月別トレンド（折りたたみ）
    with st.expander("📈 月別トレンド（全期間）", expanded=False):
        df_trend = df_all.copy()
        if sel_clients_t3:
            df_trend = df_trend[df_trend[COL_CLIENT].isin(sel_clients_t3)]
        trend = df_trend.groupby(["年月", COL_DASH]).agg(
            送付件数=(COL_SENDS, "sum"),
            開封数=(COL_OPENS, "sum"),
            クリック数=(COL_CLICKS, "sum"),
            CV数=(COL_CVS, "sum"),
        ).reset_index()
        trend["開封率"]    = trend.apply(lambda r: r["開封数"]/r["送付件数"]*100    if r["送付件数"] > 0 else 0, axis=1)
        trend["クリック率"]= trend.apply(lambda r: r["クリック数"]/r["送付件数"]*100 if r["送付件数"] > 0 else 0, axis=1)

        tg1, tg2 = st.columns(2)
        with tg1:
            fig_tr = px.bar(trend, x="年月", y="送付件数", color=COL_DASH,
                            barmode="group", title="月別 合計送付件数",
                            color_discrete_map=DASH_COLORS)
            fig_tr.update_layout(xaxis_title="")
            st.plotly_chart(fig_tr, use_container_width=True)
        with tg2:
            fig_rt = go.Figure()
            for dash in DASH_ORDER:
                d = trend[trend[COL_DASH] == dash]
                if dash != "Display":
                    fig_rt.add_trace(go.Scatter(
                        x=d["年月"], y=d["開封率"],
                        mode="lines+markers", name=f"開封率[{dash}]",
                        line=dict(color=DASH_COLORS[dash])
                    ))
                fig_rt.add_trace(go.Scatter(
                    x=d["年月"], y=d["クリック率"],
                    mode="lines+markers", name=f"CR[{dash}]",
                    line=dict(color=DASH_COLORS[dash], dash="dash")
                ))
            fig_rt.update_layout(title="月別 開封率 / CR 推移", xaxis_title="", yaxis_title="%")
            st.plotly_chart(fig_rt, use_container_width=True)


# ════════════════════════════════════
#  TAB 4: 週次レポート
# ════════════════════════════════════
with tab4:
    st.subheader("週次レポート")

    # 利用可能な週（月曜始まり）を列挙
    def week_monday(d):
        return d - timedelta(days=d.weekday())

    all_mondays = sorted(
        {week_monday(d) for d in df_all[COL_DATE].dt.date.unique()},
        reverse=True,
    )
    # 完全な週（月〜日7日分）のみ表示、かつ今週は除く
    today = date.today()
    this_monday = week_monday(today)
    full_weeks = [m for m in all_mondays if m < this_monday]

    if not full_weeks:
        st.warning("週次レポートを表示するには先週分以前のデータが必要です。")
    else:
        week_labels = {m: f"{m.strftime('%Y/%m/%d')}（月）〜{(m + timedelta(days=6)).strftime('%m/%d')}（日）" for m in full_weeks}
        sel_monday = st.selectbox(
            "対象週",
            options=full_weeks,
            format_func=lambda m: week_labels[m],
            index=0,
            key="t4_week",
        )
        sel_sunday = sel_monday + timedelta(days=6)
        prev_monday = sel_monday - timedelta(days=7)
        prev_sunday = sel_monday - timedelta(days=1)

        def week_df(start, end):
            mask = (df_all[COL_DATE].dt.date >= start) & (df_all[COL_DATE].dt.date <= end)
            return df_all[mask].copy()

        df_w  = week_df(sel_monday, sel_sunday)
        df_pw = week_df(prev_monday, prev_sunday)

        def agg_kpi(df):
            s  = int(df[COL_SENDS].sum())
            o  = int(df[COL_OPENS].sum())
            cl = int(df[COL_CLICKS].sum())
            cv = int(df[COL_CVS].sum())
            rv = int(df[COL_REV].sum())
            cost = rv * 0.1
            cpa  = cost / cv if cv > 0 else 0
            cvr_send  = cv / s  * 100 if s  > 0 else 0
            cvr_click = cv / cl * 100 if cl > 0 else 0
            open_rate  = o  / s  * 100 if s  > 0 else 0
            click_rate = cl / s  * 100 if s  > 0 else 0
            return dict(sends=s, opens=o, clicks=cl, cv=cv, rev=rv,
                        cost=cost, cpa=cpa, cvr_send=cvr_send, cvr_click=cvr_click,
                        open_rate=open_rate, click_rate=click_rate)

        kw  = agg_kpi(df_w)
        kpw = agg_kpi(df_pw)

        def delta_str(cur, prev, fmt="+.1f", suffix="%"):
            if prev == 0:
                return None
            d = (cur - prev) / prev * 100
            return f"{d:{fmt}}{suffix}"

        def change_label(cur, prev, higher_is_better=True):
            if prev == 0:
                return ""
            pct = (cur - prev) / prev * 100
            if higher_is_better:
                if pct >= 10:   return "🔺 大幅増加"
                if pct >= 3:    return "↑ 増加"
                if pct >= -3:   return "➡ ほぼ横ばい"
                if pct >= -10:  return "↓ 微減"
                return "🔻 大幅減少"
            else:
                if pct <= -10:  return "🔺 大幅改善"
                if pct <= -3:   return "↑ 改善"
                if pct <= 3:    return "➡ ほぼ横ばい"
                if pct <= 10:   return "↓ 微増"
                return "🔻 大幅悪化"

        # ── KPIサマリー ──
        st.markdown(f"#### {week_labels[sel_monday]}")
        st.caption("全クライアント・全ダッシュボード合計")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("配信完了数",       f"{kw['sends']:,}件",  delta_str(kw['sends'],  kpw['sends'],  suffix=""))
        c2.metric("メール開封数",     f"{kw['opens']:,}件",  delta_str(kw['opens'],  kpw['opens'],  suffix=""))
        c3.metric("リンククリック数", f"{kw['clicks']:,}件", delta_str(kw['clicks'], kpw['clicks'], suffix=""))
        c4.metric("コンバージョン数", f"{kw['cv']:,}件",     delta_str(kw['cv'],     kpw['cv'],     suffix=""))
        c5.metric("CV金額",           f"¥{kw['rev']:,}",    delta_str(kw['rev'],    kpw['rev'],    suffix=""))

        c6, c7, c8, c9, _ = st.columns(5)
        c6.metric("コスト(10%)",  f"¥{kw['cost']:,.0f}")
        c7.metric("CPA",          f"¥{kw['cpa']:,.0f}")
        c8.metric("開封率",       f"{kw['open_rate']:.1f}%",   f"{kw['open_rate']-kpw['open_rate']:+.1f}pt")
        c9.metric("配信起点CVR",  f"{kw['cvr_send']:.2f}%",    f"{kw['cvr_send']-kpw['cvr_send']:+.2f}pt")
        st.divider()

        # ── 先週比テーブル ──
        st.markdown("#### 先週比サマリー")
        metrics_table = [
            ("送付数",              kw['sends'],      kpw['sends'],      True,  "{:,}件"),
            ("開封率",              kw['open_rate'],  kpw['open_rate'],  True,  "{:.1f}%"),
            ("リンククリック率",    kw['click_rate'], kpw['click_rate'], True,  "{:.1f}%"),
            ("コンバージョン数",    kw['cv'],         kpw['cv'],         True,  "{:,}件"),
            ("クリック起点CVR",     kw['cvr_click'],  kpw['cvr_click'],  True,  "{:.2f}%"),
            ("配信完了起点CVR",     kw['cvr_send'],   kpw['cvr_send'],   True,  "{:.2f}%"),
            ("CV金額",              kw['rev'],        kpw['rev'],        True,  "¥{:,}"),
            ("CPA",                 kw['cpa'],        kpw['cpa'],        False, "¥{:,.0f}"),
        ]
        tbl_rows = []
        for label, cur, prev, hib, fmt in metrics_table:
            diff = cur - prev
            diff_pct = (diff / prev * 100) if prev != 0 else 0
            tbl_rows.append({
                "指標":   label,
                "先週":   fmt.format(prev),
                "今週":   fmt.format(cur),
                "差分":   f"{diff_pct:+.1f}%",
                "評価":   change_label(cur, prev, hib),
            })
        st.dataframe(
            pd.DataFrame(tbl_rows),
            use_container_width=True,
            hide_index=True,
            column_config={
                "指標": st.column_config.TextColumn("指標", width=160),
                "先週": st.column_config.TextColumn("先週", width=120),
                "今週": st.column_config.TextColumn("今週", width=120),
                "差分": st.column_config.TextColumn("増減率", width=90),
                "評価": st.column_config.TextColumn("評価", width=120),
            },
        )
        st.divider()

        # ── ダッシュボード別内訳 ──
        st.markdown("#### ダッシュボード別内訳")
        for dash in DASH_ORDER:
            dfw  = df_w[df_w[COL_DASH] == dash]
            dfpw = df_pw[df_pw[COL_DASH] == dash]
            if dfw.empty:
                continue
            label_cls = dash.lower() + "-label"
            st.markdown(f'<span class="block-label {label_cls}">{dash}</span>', unsafe_allow_html=True)

            agg = dfw.groupby(COL_CLIENT).agg(
                送付件数=(COL_SENDS, "sum"),
                開封数=(COL_OPENS, "sum"),
                クリック数=(COL_CLICKS, "sum"),
                CV数=(COL_CVS, "sum"),
                CV金額=(COL_REV, "sum"),
            ).reset_index().sort_values("送付件数", ascending=False)

            agg_p = dfpw.groupby(COL_CLIENT).agg(先週送付=(COL_SENDS, "sum")).reset_index() if not dfpw.empty else pd.DataFrame(columns=[COL_CLIENT, "先週送付"])
            agg = pd.merge(agg, agg_p, on=COL_CLIENT, how="left")
            agg["先週送付"] = agg["先週送付"].fillna(0).astype(int)
            agg["前週比"] = agg.apply(
                lambda r: f"{(r['送付件数'] - r['先週送付']) / r['先週送付'] * 100:+.1f}%" if r["先週送付"] > 0 else "-", axis=1
            )
            if dash != "Display":
                agg["開封率"] = agg.apply(lambda r: f"{r['開封数']/r['送付件数']*100:.1f}%" if r["送付件数"] > 0 else "-", axis=1)
            agg["CR"] = agg.apply(lambda r: f"{r['クリック数']/r['送付件数']*100:.1f}%" if r["送付件数"] > 0 else "-", axis=1)
            agg["CVR"] = agg.apply(lambda r: f"{r['CV数']/r['送付件数']*100:.2f}%" if r["送付件数"] > 0 else "-", axis=1)

            show = [COL_CLIENT, "送付件数", "先週送付", "前週比"]
            if dash != "Display":
                show += ["開封数", "開封率"]
            show += ["クリック数", "CR", "CV数", "CVR", "CV金額"]
            st.dataframe(
                agg[show].reset_index(drop=True),
                use_container_width=True, hide_index=True,
                column_config={
                    "送付件数":  st.column_config.NumberColumn("今週送付", format="%d"),
                    "先週送付":  st.column_config.NumberColumn("先週送付", format="%d"),
                    "開封数":    st.column_config.NumberColumn("開封",     format="%d"),
                    "クリック数": st.column_config.NumberColumn("Click",   format="%d"),
                    "CV数":      st.column_config.NumberColumn("CV",       format="%d"),
                    "CV金額":    st.column_config.NumberColumn("CV金額",   format="%d"),
                },
            )
            st.divider()


# ─── フッター ───
st.divider()
st.caption(f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M')} | データ件数: {len(df_all):,}行")
