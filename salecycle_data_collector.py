"""
SaleCycle Daily Data Collector
Extracts Sends/Opens/Clicks/Conversions for all clients from Looker dashboards
and appends results to an Excel file.
"""
import os
import sys
import time
import json
import datetime
import pandas as pd
import requests as req_lib
from playwright.sync_api import sync_playwright
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# --- Configuration ---
EMAIL = os.environ.get("SALECYCLE_USER", "s.katano@clovertech.jp")
PASSWORD = os.environ.get("SALECYCLE_PASS", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
EXCEL_INPUT = os.path.join(os.path.dirname(__file__), "salecycle動作確認.xlsx")
EXCEL_OUTPUT = os.path.join(os.path.dirname(__file__), "salecycle_daily_report.xlsx")
LOOKER_API_BASE = "https://looker-api.salecycle.com/api/internal"
MY_SALECYCLE_BASE = "https://my.salecycle.com"

# Date range: set dynamically in main() as absolute date (e.g. "2026-03-15 to 2026-03-15")
DATE_RANGE = ""
DATE_GRANULARITY = "Day"
CURRENCY = "JPY"

# Dashboard URLs
DASHBOARD_URLS = {
    "basket": f"{MY_SALECYCLE_BASE}/dashboard/new_business_aggregates::basket_abandonment__campaign_aggregates",
    "browse": f"{MY_SALECYCLE_BASE}/dashboard/new_business_aggregates::browse_abandonment__campaign_aggregates",
    "display": f"{MY_SALECYCLE_BASE}/dashboard/new_business_aggregates::display_only__campaign_aggregates",
    "landing": f"{MY_SALECYCLE_BASE}/dashboard/new_business_aggregates::msc_client_landing_page",
}

# Metric fields in query results
METRIC_FIELDS = {
    "basket": {
        "sends": "campaign_aggregates.m_sends",
        "opens": "campaign_aggregates.m_opens",
        "clicks": "campaign_aggregates.m_clicks",
        "conversions": "campaign_aggregates.m_dispatch_conversions",
    },
    "browse": {
        "sends": "campaign_aggregates.m_sends",
        "opens": "campaign_aggregates.m_opens",
        "clicks": "campaign_aggregates.m_clicks",
        "conversions": "campaign_aggregates.m_dispatch_conversions",
    },
    "display": {
        "sends": "campaign_aggregates.m_displays",
        "opens": None,
        "clicks": "campaign_aggregates.m_display_clicks",
        "conversions": "campaign_aggregates.m_display_conversions",
    },
    "landing": {
        "abandoned": "new_business_aggregates.m_abandoned_sessions_identified",
        "browse_id": "new_business_aggregates.m_browse_sessions_identified",
        "bounce":    "new_business_aggregates.m_bounce_sessions_identified",
        "purchased": "new_business_aggregates.m_purchased_sessions_identified",
    },
}


def login_and_get_session(page):
    """Log in to my.salecycle.com"""
    print("Logging in to my.salecycle.com...")
    page.goto(MY_SALECYCLE_BASE)
    page.wait_for_load_state("networkidle", timeout=30000)
    time.sleep(2)
    page.fill('input[type="email"]', EMAIL)
    page.fill('input[type="password"]', PASSWORD)
    page.click('button:has-text("Sign in")')
    try:
        page.wait_for_url(lambda url: url != f"{MY_SALECYCLE_BASE}/", timeout=20000)
    except:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except:
        pass
    time.sleep(3)
    print(f"Logged in: {page.url}")


def capture_all_templates(context, page, needed_dashboards):
    """Navigate to each dashboard and capture querymanager request bodies.
    Uses a single global listener. Requests fire ~90-120s after navigation,
    so we navigate all dashboards first, then wait for all captures."""
    templates = {}

    def on_request(req):
        if "querymanager/queries" not in req.url or req.method != "POST":
            return
        try:
            body_text = req.post_data
        except Exception:
            return
        if not body_text:
            print(f"  [qm] querymanager POST with no body")
            return
        try:
            body = json.loads(body_text)
            ctx = body.get("context", {}).get("id", "")
            print(f"  [qm] ctx={ctx}")
            for dtype in ["basket", "browse", "display"]:
                if dtype in ctx and dtype not in templates:
                    templates[dtype] = body
                    print(f"  Captured {dtype} template (ctx={ctx})")
        except Exception as e:
            print(f"  [qm] error: {e}")

    context.on("request", on_request)

    # Navigate to each dashboard to trigger Looker embed loading
    for dash_type in sorted(needed_dashboards):
        print(f"Loading {dash_type} dashboard...")
        page.goto(DASHBOARD_URLS[dash_type])
        try:
            page.wait_for_selector("iframe:not([aria-hidden])", timeout=60000)
        except Exception:
            pass
        time.sleep(10)  # Brief pause before next navigation

    # Wait up to 5 minutes total for all templates to be captured
    print("Waiting for all dashboard templates (up to 5 min)...")
    deadline = time.time() + 300
    last_count = 0
    while time.time() < deadline and len(templates) < len(needed_dashboards):
        time.sleep(5)
        if len(templates) > last_count:
            print(f"  Templates so far: {sorted(templates.keys())}")
            last_count = len(templates)

    context.remove_listener("request", on_request)

    for dtype in sorted(needed_dashboards):
        if dtype in templates:
            print(f"  {dtype}: captured ({len(templates[dtype].get('saved_queries', []))} queries)")
        else:
            print(f"  WARNING: No template for {dtype}")
    return templates


def extract_field_value(row, field_name):
    """Extract numeric value from a row field, handling pivoted data structure."""
    val = row.get(field_name)
    if val is None:
        return 0
    if isinstance(val, dict):
        # Direct value (non-pivoted)
        if "value" in val:
            return val.get("value") or 0
        # Pivoted: {pivot_key: {"value": X}, ...} — sum across all pivot buckets
        total = 0
        for pivot_val in val.values():
            if isinstance(pivot_val, dict):
                total += pivot_val.get("value") or 0
        return total
    return val or 0


def extract_metrics_from_result(result_data, dashboard_type):
    """Extract and sum metrics from a complete query result dict."""
    fields = METRIC_FIELDS[dashboard_type]
    totals = {k: 0 for k in fields}
    rows = (result_data.get("data") or {}).get("data") or []
    for field_key, field_name in fields.items():
        if field_name:
            total = sum(extract_field_value(row, field_name) for row in rows)
            if total > 0:
                totals[field_key] = total
    return totals


def parse_ndjson_response(text, dashboard_type):
    """Parse streaming NDJSON response and sum metrics across all rows"""
    fields = METRIC_FIELDS[dashboard_type]
    totals = {k: 0 for k in fields}

    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except:
            continue

        if data.get("status") != "complete":
            continue

        partial = extract_metrics_from_result(data, dashboard_type)
        for k, v in partial.items():
            if v > 0:
                totals[k] = v

    return totals


def fetch_metrics_for_client(session, headers, base_body, client_name, dashboard_type):
    """Make API call for a specific client and extract metrics"""
    if not base_body:
        return None

    import copy
    modified_body = copy.deepcopy(base_body)
    if "options" in modified_body:
        modified_body["options"]["force_run"] = True

    # Filter to only the element with date-based sorts (main metrics chart)
    all_sqs = modified_body.get("saved_queries", [])
    date_sqs = [sq for sq in all_sqs
                if any("date" in s for s in sq.get("sorts", []))]
    if date_sqs:
        modified_body["saved_queries"] = [date_sqs[0]]
    elif all_sqs:
        modified_body["saved_queries"] = [all_sqs[0]]

    for sq in modified_body.get("saved_queries", []):
        for f in sq.get("filters", []):
            if "new_clients.client_name" in f:
                f["new_clients.client_name"] = client_name
            if "new_currency_exchange_rates.currency_exchange" in f:
                f["new_currency_exchange_rates.currency_exchange"] = CURRENCY
            if "campaign_aggregates.time_slice" in f:
                f["campaign_aggregates.time_slice"] = DATE_RANGE
            if "campaign_aggregates.date_granularity" in f:
                f["campaign_aggregates.date_granularity"] = DATE_GRANULARITY
            if "new_business_aggregates.time_slice" in f:
                f["new_business_aggregates.time_slice"] = DATE_RANGE
            if "new_business_aggregates.date_granularity" in f:
                f["new_business_aggregates.date_granularity"] = DATE_GRANULARITY

    try:
        resp = session.post(
            f"{LOOKER_API_BASE}/querymanager/queries",
            headers=headers,
            json=modified_body,
            timeout=30
        )
        # Update CSRF token from response cookies
        for c in resp.cookies:
            if c.name == "CSRF-TOKEN":
                headers["X-CSRF-Token"] = c.value
        if resp.status_code != 200:
            print(f"    API error {resp.status_code}: {resp.text[:100]}")
            return None
    except Exception as e:
        print(f"    Request error: {e}")
        return None

    # Parse initial response lines; collect pending query IDs
    get_hdrs = {k: v for k, v in headers.items() if k != "Content-Type"}
    totals = {k: 0 for k in METRIC_FIELDS[dashboard_type]}

    for line in resp.text.split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except:
            continue

        if item.get("status") == "complete":
            partial = extract_metrics_from_result(item, dashboard_type)
            for k, v in partial.items():
                if v > 0:
                    totals[k] = v
            continue

        qid = item.get("id")
        if not qid:
            continue

        # Poll until complete
        deadline = time.time() + 90
        while time.time() < deadline:
            time.sleep(2)
            try:
                pr = session.get(
                    f"{LOOKER_API_BASE}/querymanager/queries/{qid}",
                    headers=get_hdrs, timeout=30
                )
                if pr.status_code == 200:
                    pd_data = pr.json()
                    if isinstance(pd_data, dict) and pd_data.get("status") == "complete":
                        partial = extract_metrics_from_result(pd_data, dashboard_type)
                        for k, v in partial.items():
                            if v > 0:
                                totals[k] = v
                        break
            except Exception as e:
                print(f"    Poll error: {e}")

    return totals


def read_clients_from_excel():
    """Read client list from Excel file"""
    df = pd.read_excel(EXCEL_INPUT, header=1)  # Row 1 is header (Client, Basket, Browse, Display)
    clients = []
    for _, row in df.iterrows():
        name = str(row.iloc[0]).strip()
        if not name or name == "nan" or name == "クライアント":
            continue
        basket_url = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
        browse_url = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ""
        display_url = str(row.iloc[3]).strip() if pd.notna(row.iloc[3]) else ""

        dashboards = []
        if basket_url and basket_url != "nan":
            dashboards.append("basket")
        if browse_url and browse_url != "nan":
            dashboards.append("browse")
        if display_url and display_url != "nan":
            dashboards.append("display")

        if dashboards:
            clients.append({"name": name, "dashboards": dashboards})

    return clients


def save_to_excel(results, report_date):
    """Save or append results to Excel report (deduplicates by date+client+dashboard)"""
    dashboard_labels = {"basket": "Basket", "browse": "Browse", "display": "Display"}

    try:
        wb = load_workbook(EXCEL_OUTPUT)
        ws = wb.active
        # Build set of already-written (date, client, dashboard) keys to avoid duplicates
        existing = set()
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0] and row[1] and row[2]:
                existing.add((str(row[0]), str(row[1]), str(row[2])))
    except Exception:
        wb = Workbook()
        ws = wb.active
        ws.title = "Daily Report"
        headers = ["日付", "クライアント", "ダッシュボード種別", "送付件数", "開封数", "クリック数", "コンバージョン数", "識別数"]
        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.fill = PatternFill("solid", start_color="1F4E79")
            cell.font = Font(bold=True, color="FFFFFF", name="Arial")
            cell.alignment = Alignment(horizontal="center")
        existing = set()

    next_row = ws.max_row + 1
    added = 0

    for item in results:
        label = dashboard_labels.get(item["dashboard"], item["dashboard"])
        key = (str(report_date), item["client"], label)
        if key in existing:
            continue  # Skip duplicate
        existing.add(key)
        row_data = [
            report_date,
            item["client"],
            label,
            item.get("sends", 0),
            item.get("opens", "") if item.get("opens") is not None else "",
            item.get("clicks", 0),
            item.get("conversions", 0),
            item.get("visitors_identified", ""),
        ]
        for col, val in enumerate(row_data, 1):
            cell = ws.cell(row=next_row, column=col, value=val)
            cell.font = Font(name="Arial", size=10)
            cell.alignment = Alignment(horizontal="center")
        next_row += 1
        added += 1

    print(f"Saving {added} new rows to Excel...")

    # Auto-fit columns
    for col in ws.columns:
        max_len = max(len(str(cell.value or "")) for cell in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)

    wb.save(EXCEL_OUTPUT)
    print(f"Saved to {EXCEL_OUTPUT}")

    # CSVにも保存してGitHubにpush
    try:
        import pandas as _pd, subprocess as _sp
        _csv_path = os.path.join(os.path.dirname(__file__), "salecycle_daily_report.csv")
        _df = _pd.read_excel(EXCEL_OUTPUT, engine="openpyxl")
        _df.to_csv(_csv_path, index=False, encoding="utf-8-sig")
        _repo = os.path.dirname(__file__)
        _sp.run(["git", "-C", _repo, "add", "salecycle_daily_report.csv"], check=True)
        _sp.run(["git", "-C", _repo, "commit", "-m", f"data: {report_date}"], check=True)
        _sp.run(["git", "-C", _repo, "push"], check=True)
        print(f"CSV updated and pushed to GitHub ({report_date})")
    except Exception as _e:
        print(f"CSV/GitHub push skipped: {_e}")


def get_previous_sends(report_date):
    """Excelから report_date の前日の送付件数を {(client, dashboard_label): sends} で返す"""
    prev_date = (datetime.datetime.strptime(report_date, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    prev_sends = {}
    if not os.path.exists(EXCEL_OUTPUT):
        return prev_sends
    try:
        wb = load_workbook(EXCEL_OUTPUT, read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            date_val, client, dashboard, sends = row[0], row[1], row[2], row[3]
            if str(date_val) == prev_date and client and dashboard:
                prev_sends[(client, dashboard)] = sends if isinstance(sends, (int, float)) else 0
        wb.close()
    except Exception as e:
        print(f"  [Slack] 前日データ読み込みエラー: {e}")
    return prev_sends


def send_slack_report(alerts, results, report_date):
    """日次レポートをSlackに送信する（アラートの有無に関わらず常に送信）"""
    if not SLACK_WEBHOOK_URL:
        print("  [Slack] SLACK_WEBHOOK_URL \u304c\u672a\u8a2d\u5b9a\u306e\u305f\u3081\u901a\u77e5\u3092\u30b9\u30ad\u30c3\u30d7")
        return

    total_sends = sum(
        int(item["sends"]) for item in results
        if isinstance(item.get("sends"), (int, float))
    )
    client_count = len(set(item["client"] for item in results))

    excel_path = EXCEL_OUTPUT.replace("\\", "/")
    excel_link = f"file:///{excel_path}"

    lines = [
        f":bar_chart: *SaleCycle \u65e5\u6b21\u30ec\u30dd\u30fc\u30c8 ({report_date})*",
        f"\u51e6\u7406\u30af\u30e9\u30a4\u30a2\u30f3\u30c8\u6570: {client_count}\u4ef6 | \u5408\u8a08\u9001\u4ed8\u4ef6\u6570: {total_sends:,}\u4ef6",
        "",
    ]

    if alerts:
        lines.append(f":warning: *\u30a2\u30e9\u30fc\u30c8 {len(alerts)}\u4ef6:*")
        for a in alerts:
            if a["reason"] == "fetch_failed":
                lines.append(f"- {a['client']} [{a['dashboard']}]: :x: *データ取得失敗* \uff08\u524d\u65e5: {a['prev']:,}\u4ef6\uff09")
            elif a["reason"] == "zero":
                lines.append(f"- {a['client']} [{a['dashboard']}]: *0\u4ef6* \uff08\u524d\u65e5: {a['prev']:,}\u4ef6\uff09")
            else:
                lines.append(
                    f"- {a['client']} [{a['dashboard']}]: {a['today']:,}\u4ef6 "
                    f"\uff08\u524d\u65e5: {a['prev']:,}\u4ef6 / {a['change_pct']:+.1f}%\uff09"
                )
    else:
        lines.append(":white_check_mark: \u7570\u5e38\u306a\u3057")

    lines.append("")
    lines.append(f":open_file_folder: <{excel_link}|Excel\u30ec\u30dd\u30fc\u30c8\u3092\u958b\u304f>")

    payload = {"text": "\n".join(lines)}
    try:
        resp = req_lib.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            alert_msg = f"{len(alerts)}\u4ef6\u306e\u30a2\u30e9\u30fc\u30c8\u3042\u308a" if alerts else "\u7570\u5e38\u306a\u3057"
            print(f"  [Slack] \u30ec\u30dd\u30fc\u30c8\u3092\u9001\u4fe1\u3057\u307e\u3057\u305f ({alert_msg})")
        else:
            print(f"  [Slack] \u9001\u4fe1\u5931\u6557: {resp.status_code} {resp.text[:80]}")
    except Exception as e:
        print(f"  [Slack] \u9001\u4fe1\u30a8\u30e9\u30fc: {e}")


def check_sends_alerts(results, report_date):
    """\u9001\u4ed8\u4ef6\u6570\u30c1\u30a7\u30c3\u30af\u30fb\u65e5\u6b21\u30ec\u30dd\u30fc\u30c8\u3092Slack\u306b\u9001\u4fe1\u3059\u308b"""
    dashboard_labels = {"basket": "Basket", "browse": "Browse", "display": "Display"}
    prev_sends = get_previous_sends(report_date)
    alerts = []

    for item in results:
        today_sends = item.get("sends")
        label = dashboard_labels.get(item["dashboard"], item["dashboard"])
        key = (item["client"], label)
        prev = prev_sends.get(key)

        if today_sends == "" or today_sends is None:
            # 取得失敗
            alerts.append({
                "client": item["client"], "dashboard": label,
                "today": None, "prev": prev if prev is not None else 0,
                "reason": "fetch_failed", "change_pct": None,
            })
            continue

        if not isinstance(today_sends, (int, float)):
            continue
        today_sends = int(today_sends)

        if today_sends == 0:
            alerts.append({
                "client": item["client"], "dashboard": label,
                "today": 0, "prev": prev if prev is not None else 0,
                "reason": "zero", "change_pct": -100.0,
            })
        elif prev is not None and prev > 0:
            change_pct = (today_sends - prev) / prev * 100
            if change_pct <= -20:
                alerts.append({
                    "client": item["client"], "dashboard": label,
                    "today": today_sends, "prev": int(prev),
                    "reason": "drop", "change_pct": change_pct,
                })

    if alerts:
        print(f"\nSlack\u30a2\u30e9\u30fc\u30c8\u5bfe\u8c61: {len(alerts)} \u4ef6")
        for a in alerts:
            if a["reason"] == "fetch_failed":
                reason = "取得失敗"
            elif a["reason"] == "zero":
                reason = "0件"
            else:
                reason = f"{a['change_pct']:+.1f}%"
            print(f"  {a['client']} [{a['dashboard']}]: {reason}")
    else:
        print("\nSlack\u30a2\u30e9\u30fc\u30c8: \u7570\u5e38\u306a\u3057")

    send_slack_report(alerts, results, report_date)


def find_missing_dates(days_back=7):
    """過去days_back日間でExcelにデータがない日付を (days_ago, date_str) のリストで返す。
    16時前は昨日（days_ago=1）をスキップ: PM3:00更新前のため不完全データになるのを防ぐ。
    16時以降は昨日も含める（16時タスクが失敗した場合のフォールバック）。
    """
    now = datetime.datetime.now()
    today = now.date()
    # 16:00より前は昨日をスキップ（16時タスクに任せる）
    min_days_ago = 2 if now.hour < 16 else 1

    existing_dates = set()

    if os.path.exists(EXCEL_OUTPUT):
        try:
            wb = load_workbook(EXCEL_OUTPUT, read_only=True)
            ws = wb.active
            for row in ws.iter_rows(min_row=2, values_only=True):
                if row[0]:
                    existing_dates.add(str(row[0])[:10])  # YYYY-MM-DD
            wb.close()
        except Exception as e:
            print(f"  [Backfill] Excel読み込みエラー: {e}")

    missing = []
    for days_ago in range(min_days_ago, days_back + 1):
        target = today - datetime.timedelta(days=days_ago)
        target_str = target.strftime("%Y-%m-%d")
        if target_str not in existing_dates:
            missing.append((days_ago, target_str))

    return missing


def load_templates_from_files():
    """Load dashboard request templates from pre-captured JSON files."""
    templates = {}
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for dtype, fname in [("basket", "basket_req.json"), ("browse", "browse_req.json"),
                         ("display", "display_req.json"), ("landing", "landing_identified_req.json")]:
        path = os.path.join(script_dir, fname)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                templates[dtype] = json.load(f)
            sqs = len(templates[dtype].get("saved_queries", []))
            print(f"  Loaded {dtype} template ({sqs} queries)")
        else:
            print(f"  WARNING: {fname} not found")
    return templates


def get_looker_session():
    """ブラウザでログインしてLookerセッション（requests.Session, headers）を返す"""
    looker_cookies = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, channel="chrome")
        context = browser.new_context()
        page = context.new_page()

        login_and_get_session(page)

        print("Establishing Looker session...")
        page.goto(DASHBOARD_URLS["basket"])
        try:
            page.wait_for_selector("iframe:not([aria-hidden])", timeout=60000)
        except Exception:
            pass
        time.sleep(15)

        cookies = context.cookies()
        looker_cookies = {c["name"]: c["value"] for c in cookies
                          if "looker-api.salecycle.com" in c.get("domain", "")}
        browser.close()

    if not looker_cookies:
        print("ERROR: No Looker session cookies obtained")
        sys.exit(1)

    print(f"Looker cookies: {list(looker_cookies.keys())}")
    session = req_lib.Session()
    session.cookies.update(looker_cookies)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "X-CSRF-Token": looker_cookies.get("CSRF-TOKEN", ""),
        "Origin": "https://looker-api.salecycle.com",
        "Referer": "https://looker-api.salecycle.com/",
    }
    return session, headers


def collect_for_date(session, headers, templates, clients, report_date, days_ago, skip_slack=False):
    """指定日のデータを収集してExcel保存・Slack通知する"""
    global DATE_RANGE
    if days_ago is None:
        today = datetime.datetime.now().date()
        target = datetime.datetime.strptime(report_date, "%Y-%m-%d").date()
        days_ago = (today - target).days
    DATE_RANGE = f"{days_ago} day{'s' if days_ago != 1 else ''} ago for 1 day"
    print(f"\n--- Collecting: {report_date} (filter: {DATE_RANGE}) ---")

    results = []
    landing_template = templates.get("landing")
    for client in clients:
        client_name = client["name"]
        print(f"\n  {client_name}:")

        # Landing: Visitors Identified (abandoned + browse_id per client)
        visitors_abandoned = 0
        visitors_browse = 0
        if landing_template:
            lm = fetch_metrics_for_client(session, headers, landing_template, client_name, "landing")
            if lm:
                visitors_abandoned = int(lm.get("abandoned", 0) or 0)
                visitors_browse    = int(lm.get("browse_id", 0) or 0)
                print(f"    landing: Abandoned={visitors_abandoned}, Browse={visitors_browse}")

        for dash_type in client["dashboards"]:
            template = templates.get(dash_type)
            if not template:
                print(f"    {dash_type}: no template available, skipping")
                continue

            metrics = fetch_metrics_for_client(session, headers, template, client_name, dash_type)
            if metrics:
                # 送付率/クリック率用: Basket→abandoned識別数, Browse→browse識別数
                if dash_type == "basket":
                    visitors_id = visitors_abandoned
                elif dash_type == "browse":
                    visitors_id = visitors_browse
                else:
                    visitors_id = ""
                result = {
                    "client": client_name,
                    "dashboard": dash_type,
                    "sends": int(metrics.get("sends", 0)),
                    "opens": int(metrics.get("opens", 0)) if metrics.get("opens") is not None else "",
                    "clicks": int(metrics.get("clicks", 0)),
                    "conversions": int(metrics.get("conversions", 0)),
                    "visitors_identified": visitors_id,
                }
                results.append(result)
                print(f"    {dash_type}: Sends={result['sends']}, Opens={result['opens']}, Clicks={result['clicks']}, Conv={result['conversions']}, Visitors={visitors_id}")
            else:
                print(f"    {dash_type}: failed to get data")
                results.append({
                    "client": client_name,
                    "dashboard": dash_type,
                    "sends": "",
                    "opens": "",
                    "clicks": "",
                    "conversions": "",
                    "visitors_identified": "",
                })

    save_to_excel(results, report_date)
    if not skip_slack:
        check_sends_alerts(results, report_date)
    return results


def backfill_from_date(from_date_str):
    """カスタムバックフィル: 指定日から昨日までの欠損データを補完する"""
    print(f"=== Custom Backfill: {from_date_str} 〜 昨日 ===")

    today = datetime.datetime.now().date()
    start = datetime.datetime.strptime(from_date_str, "%Y-%m-%d").date()
    yesterday = today - datetime.timedelta(days=1)

    # 既存データを確認
    existing_dates = set()
    if os.path.exists(EXCEL_OUTPUT):
        try:
            wb = load_workbook(EXCEL_OUTPUT, read_only=True)
            ws = wb.active
            for row in ws.iter_rows(min_row=2, values_only=True):
                if row[0]:
                    existing_dates.add(str(row[0])[:10])
            wb.close()
        except Exception as e:
            print(f"  Excel読み込みエラー: {e}")

    # 欠損日付リストを作成
    missing = []
    d = start
    while d <= yesterday:
        d_str = d.strftime("%Y-%m-%d")
        if d_str not in existing_dates:
            days_ago = (today - d).days
            missing.append((days_ago, d_str))
        d += datetime.timedelta(days=1)

    if not missing:
        print("欠損データなし - バックフィル不要")
        return

    print(f"対象日付: {len(missing)}日分 ({missing[0][1]} 〜 {missing[-1][1]})")

    clients = read_clients_from_excel()
    print(f"Clients: {len(clients)}")

    needed_dashboards = set()
    for c in clients:
        needed_dashboards.update(c["dashboards"])

    print("\nLoading dashboard templates...")
    templates = load_templates_from_files()
    for dtype in sorted(needed_dashboards):
        if dtype not in templates:
            print(f"  ERROR: No template for '{dtype}'.")

    session, headers = get_looker_session()

    for days_ago, report_date in missing:
        collect_for_date(session, headers, templates, clients, report_date, days_ago, skip_slack=True)

    print("\n=== Custom Backfill Complete ===")


def startup_backfill():
    """起動時バックフィルモード: 過去7日間の欠損データを補完する"""
    print("=== Startup Backfill Mode ===")

    missing = find_missing_dates(days_back=14)
    if not missing:
        print("欠損データなし - バックフィル不要")
        return

    print(f"欠損日付: {[d for _, d in missing]}")

    clients = read_clients_from_excel()
    print(f"Clients to process: {len(clients)}")

    needed_dashboards = set()
    for c in clients:
        needed_dashboards.update(c["dashboards"])

    print("\nLoading dashboard templates...")
    templates = load_templates_from_files()
    for dtype in sorted(needed_dashboards):
        if dtype not in templates:
            print(f"  ERROR: No template for '{dtype}'.")

    session, headers = get_looker_session()

    for days_ago, report_date in missing:
        collect_for_date(session, headers, templates, clients, report_date, days_ago)

    print("\n=== Backfill Complete ===")


def main():
    """通常モード: 昨日のデータを収集する"""
    days_ago = 1
    report_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%d")
    print(f"Collecting data for: {report_date}")

    clients = read_clients_from_excel()
    print(f"Clients to process: {len(clients)}")

    needed_dashboards = set()
    for c in clients:
        needed_dashboards.update(c["dashboards"])
    print(f"Dashboard types needed: {needed_dashboards}")

    print("\nLoading dashboard templates...")
    templates = load_templates_from_files()
    for dtype in sorted(needed_dashboards):
        if dtype not in templates:
            print(f"  ERROR: No template for '{dtype}'. Run capture_templates.py first.")

    session, headers = get_looker_session()
    collect_for_date(session, headers, templates, clients, report_date, days_ago)

    print("Done!")


def send_slack_error(error_msg, mode="main"):
    """スクリプトがエラーで落ちた時にSlackへ通知する"""
    if not SLACK_WEBHOOK_URL:
        return
    import traceback
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    text = (
        f":rotating_light: *SaleCycle \u30b9\u30af\u30ea\u30d7\u30c8\u30a8\u30e9\u30fc* ({now})\n"
        f"\u30e2\u30fc\u30c9: `{mode}`\n"
        f"```{error_msg[:800]}```"
    )
    try:
        req_lib.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception:
        pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SaleCycle Daily Data Collector")
    parser.add_argument("--startup", action="store_true",
                        help="起動時バックフィルモード: 過去7日間の欠損データを補完")
    parser.add_argument("--from-date", metavar="YYYY-MM-DD",
                        help="指定日から昨日までの欠損データを一括収集")
    args = parser.parse_args()

    if not PASSWORD:
        print("ERROR: SALECYCLE_PASS environment variable not set")
        sys.exit(1)

    if args.from_date:
        mode = "backfill"
    elif args.startup:
        mode = "startup"
    else:
        mode = "main"

    try:
        if args.from_date:
            backfill_from_date(args.from_date)
        elif args.startup:
            startup_backfill()
        else:
            main()
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        print(f"FATAL ERROR:\n{err}")
        send_slack_error(err, mode=mode)
        sys.exit(1)
