"""Static HTML run-history report. No server: open the file in a browser."""

from __future__ import annotations

import html
from datetime import datetime
from pathlib import Path

from dpp import db
from dpp.contract import load_contract
from dpp.pipeline import session
from dpp.settings import Settings

CSS = """
:root{--bg:#f7f7f5;--card:#fff;--fg:#1d1d1b;--muted:#6b6b66;--line:#e4e4df;
--ok:#1f7a4d;--okbg:#e3f3ea;--bad:#b42318;--badbg:#fdecea;--warn:#8a5a00;--warnbg:#fff4d6;--info:#3b5bdb;--infobg:#e8edff;--bar:#4c6ef5}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--card:#1e1e1c;--fg:#ececea;--muted:#9a9a94;--line:#33332f;
--ok:#5fd394;--okbg:#173626;--bad:#ff8a80;--badbg:#3d1a17;--warn:#ffd166;--warnbg:#3a2f12;--info:#9fb3ff;--infobg:#1e2647;--bar:#748ffc}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 -apple-system,system-ui,sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px 64px}h1{font-size:22px;margin:0 0 4px}h2{font-size:16px;margin:32px 0 8px}
.sub{color:var(--muted);margin:0 0 20px}.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}.kpi b{display:block;font-size:22px}
.kpi span{color:var(--muted);font-size:12px}.card{background:var(--card);border:1px solid var(--line);border-radius:10px;overflow-x:auto}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;color:var(--muted);font-weight:600}td{white-space:nowrap}td.msg{white-space:normal}td.n{text-align:right;font-variant-numeric:tabular-nums}tr:last-child td{border-bottom:0}
.pill{display:inline-block;padding:1px 8px;border-radius:99px;font-size:12px;font-weight:600}
.published{background:var(--okbg);color:var(--ok)}.quarantined,.failed{background:var(--badbg);color:var(--bad)}
.superseded,.discarded,.landed,.audited{background:var(--line);color:var(--muted)}.override{background:var(--warnbg);color:var(--warn)}
.fail{color:var(--bad);font-weight:600}.warn{color:var(--warn)}.msg{color:var(--muted);font-size:12px;max-width:420px}
details{background:var(--card);border:1px solid var(--line);border-radius:10px;margin:8px 0;padding:10px 14px}
summary{cursor:pointer;font-weight:600}.bar{height:10px;background:var(--bar);border-radius:3px}code{font-size:12px}
"""


def _e(x) -> str:
    return html.escape("" if x is None else str(x))


def _num(x) -> str:
    return "" if x is None else f"{x:,}"


def build_report(settings: Settings) -> Path:
    contract = load_contract(settings.contract_path)
    with session(settings, locked=False) as (con, _):
        batches = con.execute(
            "SELECT batch_id, month, status, mode, row_count, rejected_rows, rejected_pct, errors, warnings, "
            "contract_version, finished_at, message FROM v_batch_quality ORDER BY batch_id DESC"
        ).fetchall()
        monthly = con.execute(
            "SELECT pickup_month, trips, revenue, trips_mom_pct, revenue_mom_pct FROM v_monthly_summary "
            "ORDER BY pickup_month"
        ).fetchall()
        flagged = [b for b in batches if b[2] in ("quarantined", "discarded", "failed") or "OVERRIDE" in (b[11] or "")]
        details = {
            b[0]: con.execute(
                "SELECT check_name, severity, status, observed, threshold, message FROM check_results "
                "WHERE batch_id = ? AND status = 'fail' ORDER BY severity, check_name",
                [b[0]],
            ).fetchall()
            for b in flagged
        }
        assertions = db.run_assertions(con)

    quarantined = [b for b in batches if b[2] == "quarantined"]
    total_rows = sum(m[1] for m in monthly)
    max_trips = max((m[1] for m in monthly), default=1)

    def status_pill(status, message):
        cls = "override" if status == "published" and (message or "").startswith("OVERRIDE") else status
        label = "published (override)" if cls == "override" else status
        return f'<span class="pill {cls}">{_e(label)}</span>'

    rows = "".join(
        f"<tr><td class=n>{b[0]}</td><td>{_e(b[1])}</td><td>{status_pill(b[2], b[11])}</td><td>{_e(b[3])}</td>"
        f"<td class=n>{_num(b[4])}</td><td class=n>{_num(b[5])}</td>"
        f"<td class=n>{'' if b[6] is None else f'{b[6]:.3f}%'}</td>"
        f"<td class=n{' fail' if b[7] else ''}>{b[7]}</td><td class=n{' warn' if b[8] else ''}>{b[8]}</td>"
        f"<td class=n>v{b[9]}</td><td class=msg>{_e(b[11])}</td></tr>"
        for b in batches
    )
    month_rows = "".join(
        f"<tr><td>{m[0]:%Y-%m}</td><td class=n>{m[1]:,}</td>"
        f'<td style="width:35%"><div class=bar style="width:{100 * m[1] / max_trips:.1f}%"></div></td>'
        f"<td class=n>${m[2]:,.0f}</td><td class=n>{'' if m[3] is None else f'{m[3]:+.1f}%'}</td>"
        f"<td class=n>{'' if m[4] is None else f'{m[4]:+.1f}%'}</td></tr>"
        for m in monthly
    )
    detail_html = "".join(
        f"<details{' open' if b[2] == 'quarantined' else ''}><summary>Batch {b[0]} · {_e(b[1])} · "
        f"{status_pill(b[2], b[11])}</summary><div class=card style='margin-top:8px'><table>"
        "<tr><th>Check</th><th>Severity</th><th>Observed</th><th>Expected</th><th>Message</th></tr>"
        + "".join(
            f"<tr><td><code>{_e(c[0])}</code></td><td class={'fail' if c[1] == 'ERROR' else 'warn'}>{c[1]}</td>"
            f"<td>{_e(c[3])}</td><td>{_e(c[4])}</td><td class=msg>{_e(c[5])}</td></tr>"
            for c in details[b[0]]
        )
        + "</table></div>"
        + (f"<p class=msg>Inspect: <code>SELECT * FROM stg_trips__{b[0]} LIMIT 20</code> · "
           f"Resolve: <code>pipeline resolve {b[0]} --discard</code> or "
           f"<code>pipeline resolve {b[0]} --publish --reason \"...\"</code></p>" if b[2] == "quarantined" else "")
        + "</details>"
        for b in flagged
    )  # fmt: skip
    assert_rows = "".join(
        f"<tr><td><code>{_e(k)}</code></td><td class={'fail' if v else ''}>{'PASS' if v == 0 else f'FAIL ({v})'}"
        "</td></tr>"
        for k, v in assertions.items()
    )
    rules = "".join(f"<tr><td><code>{_e(r.name)}</code></td><td><code>{_e(r.sql)}</code></td></tr>"
                    for r in contract.row_rules)  # fmt: skip

    page = f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>Pipeline Run History</title>
<style>{CSS}</style></head><body><main>
<h1>Pipeline run history</h1>
<p class=sub>Contract <code>{_e(contract.name)}</code> v{contract.version} · generated {datetime.now():%Y-%m-%d %H:%M}</p>
<div class=kpis>
<div class=kpi><b>{len(monthly)}</b><span>months published</span></div>
<div class=kpi><b>{total_rows:,}</b><span>trips in warehouse</span></div>
<div class=kpi><b>{len(batches)}</b><span>batches processed</span></div>
<div class=kpi><b>{len(quarantined)}</b><span>awaiting a human decision</span></div>
<div class=kpi><b>{sum(1 for v in assertions.values() if v == 0)}/{len(assertions)}</b><span>warehouse assertions passing</span></div>
</div>
<h2>Needs attention</h2>{detail_html or "<p class=sub>Nothing quarantined.</p>"}
<h2>Batches</h2><div class=card><table><tr><th>ID</th><th>Month</th><th>Status</th><th>Mode</th><th>Rows</th>
<th>Rejected</th><th>Rejected %</th><th>Errors</th><th>Warnings</th><th>Contract</th><th>Message</th></tr>{rows}</table></div>
<h2>Published months</h2><div class=card><table><tr><th>Month</th><th>Trips</th><th></th><th>Revenue</th>
<th>Trips MoM</th><th>Revenue MoM</th></tr>{month_rows}</table></div>
<h2>Warehouse assertions</h2><div class=card><table><tr><th>Assertion (sql/assertions)</th><th>Result</th></tr>
{assert_rows}</table></div>
<h2>Row rules in the contract</h2><div class=card><table><tr><th>Rule</th><th>A valid row satisfies</th></tr>
{rules}</table></div>
<p class=sub style="margin-top:12px">Rows breaking a rule go to <code>rejected_rows</code>. Above
{contract.max_rejected_row_rate:.1%} rejected rows, the whole batch is quarantined.</p>
</main></body></html>"""
    settings.report_dir.mkdir(parents=True, exist_ok=True)
    out = settings.report_dir / "index.html"
    out.write_text(page)
    return out
