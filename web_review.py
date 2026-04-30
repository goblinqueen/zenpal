"""
ZenPal web interface. Start with: python zp.py serve
"""
import csv
import json
import os
import sys
import threading
import time

import pandas as pd
import zenmoney as zenmod
from dateutil.relativedelta import relativedelta
from flask import Flask, render_template_string, request, redirect, url_for, jsonify

from config import ZEN_API_TOKEN
from zp import (load_or_sync, get_updates, detect_op_files,
                find_candidates, _build_transfer, MERGE_RULES, ACCOUNT_SLOTS, DOWNLOADS)
from pipeline import Predictor, MANUAL_TAGS
from train import train_model

FILENAME   = 'zenmoney.json'
OUTPUT_CSV = 'review_output.csv'
WINDOW_PATH = 'window.json'
WINDOW_DEFAULT = {
    'train_start':   '2024-02-01',
    'train_end':     '2025-01-31',
    'predict_start': '2025-02-01',
    'predict_end':   '2025-02-28',
}

# ---------------------------------------------------------------------------
app = Flask(__name__)

zen = load_or_sync(FILENAME, ZEN_API_TOKEN)
try:
    predictor = Predictor()
except Exception:
    predictor = None

tag_map     = {t['id']: t['title'] for t in zen.tag}
tag_id_map  = {t['title']: t['id'] for t in zen.tag}
account_map = {a['id']: a['title'] for a in zen.account}
tags_sorted = sorted(zen.tag, key=lambda t: t['title'])

df_all = pd.DataFrame(zen.transaction)
df_all['date'] = pd.to_datetime(df_all['date'])


# ---------------------------------------------------------------------------
# Task runner
# ---------------------------------------------------------------------------

_task = {'running': False, 'lines': [], 'iteration': 0, 'total': 0, 'error': None}


class _Tee:
    """Forward writes to real stdout and capture lines into _task."""
    def __init__(self, real):
        self._real = real

    def write(self, s):
        self._real.write(s)
        if s.strip():
            _task['lines'].append(s.rstrip('\n'))

    def flush(self):
        self._real.flush()


def run_task(fn):
    """Run fn() in a background thread, capturing print output into _task['lines']."""
    if _task['running']:
        return False
    _task.update(running=True, lines=[], iteration=0, total=0, error=None)
    real = sys.stdout

    def _run():
        sys.stdout = _Tee(real)
        try:
            fn()
        except Exception as e:
            _task['error'] = str(e)
        finally:
            sys.stdout = real
            _task['running'] = False

    threading.Thread(target=_run, daemon=True).start()
    return True


def _reload_zen():
    """Sync from API and refresh all module-level globals."""
    global zen, df_all, tag_map, tag_id_map, account_map, tags_sorted, predictor
    zen         = load_or_sync(FILENAME, ZEN_API_TOKEN)
    tag_map     = {t['id']: t['title'] for t in zen.tag}
    tag_id_map  = {t['title']: t['id'] for t in zen.tag}
    account_map = {a['id']: a['title'] for a in zen.account}
    tags_sorted = sorted(zen.tag, key=lambda t: t['title'])
    df_all      = pd.DataFrame(zen.transaction)
    df_all['date'] = pd.to_datetime(df_all['date'])
    try:
        predictor = Predictor()
    except Exception:
        predictor = None
    return zen


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------

def load_window():
    try:
        with open(WINDOW_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return dict(WINDOW_DEFAULT)

def save_window(w):
    with open(WINDOW_PATH, 'w') as f:
        json.dump(w, f, indent=2)

def advance_window(w):
    ts = pd.Timestamp(w['predict_start']) + relativedelta(months=1)
    te = (ts + relativedelta(months=1)) - pd.Timedelta(days=1)
    return {
        'train_start':   (pd.Timestamp(w['train_start']) + relativedelta(months=1)).strftime('%Y-%m-%d'),
        'train_end':     (pd.Timestamp(w['train_end'])   + relativedelta(months=1)).strftime('%Y-%m-%d'),
        'predict_start': ts.strftime('%Y-%m-%d'),
        'predict_end':   te.strftime('%Y-%m-%d'),
    }


# ---------------------------------------------------------------------------
# Build transactions for review
# ---------------------------------------------------------------------------

def build_transactions(start_date, end_date):
    manual_ids = {t['id'] for t in zen.tag if t['title'] in MANUAL_TAGS}

    df = df_all[
        (df_all['date'] >= pd.Timestamp(start_date))
        & (df_all['date'] <= pd.Timestamp(end_date))
        & (df_all['deleted'] == False)
        & ~((df_all['income'] > 0) & (df_all['outcome'] > 0))
        & ~df_all['tag'].apply(lambda tags: bool(tags) and bool(set(tags) & manual_ids))
    ].copy().sort_values('date')

    if df.empty:
        return []

    df = predictor.tag(df, zen)

    rows = []
    for txn in df.to_dict('records'):
        final         = txn.get('final_tag')
        existing_tags = txn.get('tag') or []
        rows.append({
            'id':             txn['id'],
            'date':           txn['date'].strftime('%Y-%m-%d'),
            'amount':         f"{'-' if txn['outcome'] > 0 else '+'}{txn['outcome'] if txn['outcome'] > 0 else txn['income']:.2f}",
            'account':        account_map.get(txn.get('outcomeAccount') or txn.get('incomeAccount'), '?'),
            'payee':          txn.get('originalPayee') or txn.get('payee') or '',
            'comment':        txn.get('comment') or '',
            'existing_tag':   tag_map.get(existing_tags[0], '') if existing_tags else '',
            'predicted_tag':  final or '',
            'predicted_name': tag_map.get(final, '') if final else '—',
            'confidence':     f'{txn["final_confidence"]:.0%}',
            'conf_value':     txn['final_confidence'],
        })
    return rows


# ---------------------------------------------------------------------------
# Form / CSV / upload helpers
# ---------------------------------------------------------------------------

def parse_form(form):
    csv_rows, tag_updates = [], []
    for key, tag_id in form.items():
        if not key.startswith('tag_'):
            continue
        txn_id       = key[4:]
        new_tag_name = form.get(f'new_{txn_id}', '').strip()
        pred_tag_id  = form.get(f'pred_{txn_id}', '')

        if new_tag_name:
            reviewed_name = new_tag_name
            reviewed_id   = tag_id_map.get(new_tag_name)
        elif tag_id:
            reviewed_name = tag_map.get(tag_id, '')
            reviewed_id   = tag_id
        else:
            reviewed_name = ''
            reviewed_id   = None

        csv_rows.append({
            'transaction_id': txn_id,
            'predicted_cat':  tag_map.get(pred_tag_id, '') if pred_tag_id else '',
            'reviewed_cat':   reviewed_name,
        })
        if reviewed_id:
            tag_updates.append((txn_id, reviewed_id))
    return csv_rows, tag_updates


def save_csv(rows):
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['transaction_id', 'predicted_cat', 'reviewed_cat'])
        writer.writeheader()
        writer.writerows(rows)


def sync_tags(tag_updates):
    transactions = []
    for txn_id, tid in tag_updates:
        diff = zen.set_tags(txn_id, [tid])
        transactions.extend(diff['transaction'])
    if not transactions:
        return 0
    conn = zenmod.ZenConnection(ZEN_API_TOKEN)
    conn.sync_timestamp = zen.server_timestamp
    diff = conn.sync({'transaction': transactions})
    zen.apply_diff(diff)
    zen.write(FILENAME)
    return len(transactions)


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: sans-serif; font-size: 13px; background: #f0f0f0; }
a { color: inherit; text-decoration: none; }
.header { background: #1a1a2e; color: #fff; padding: 0 20px;
          display: flex; align-items: center; height: 44px; gap: 14px; }
.header-title { font-size: 16px; font-weight: bold; flex: 1; }
.header-link { color: #7ecfff; font-size: 12px; }
.header-link:hover { color: #fff; }
.btn-stop { background: #6b0000; color: #fff; border: none; border-radius: 4px;
            padding: 4px 12px; cursor: pointer; font-size: 12px; }
.btn-stop:hover { background: #a00; }
.tabs { background: #222; display: flex; padding: 0 16px; border-bottom: 2px solid #111; }
.tab { padding: 9px 18px; color: #999; font-size: 13px; border-bottom: 2px solid transparent;
       margin-bottom: -2px; cursor: pointer; }
.tab:hover { color: #fff; }
.tab.active { color: #fff; border-bottom-color: #2a7ae2; }
.flash { padding: 8px 20px; background: #d4edda; color: #155724; font-size: 12px; font-weight: bold; }
.flash.err { background: #f8d7da; color: #721c24; }
.btn { padding: 6px 14px; border: none; border-radius: 4px; cursor: pointer; font-size: 12px; }
.btn:disabled { opacity: .5; cursor: default; }
.btn-primary   { background: #2a7ae2; color: #fff; }
.btn-success   { background: #1a7a3a; color: #fff; }
.btn-secondary { background: #555; color: #fff; }
.btn-warn      { background: #7a4a00; color: #fff; }
.btn-danger    { background: #c0392b; color: #fff; }
.btn-purple    { background: #4a007a; color: #fff; }
"""

_NAV = """
<div class="header">
  <span class="header-title">ZenPal</span>
  <a href="https://zenmoney.ru/a/#" target="_blank" class="header-link">ZenMoney ↗</a>
  <form method="post" action="/shutdown" style="margin:0">
    <button class="btn-stop" onclick="return confirm('Stop the server?')">■ Stop</button>
  </form>
</div>
<div class="tabs">
  <a href="/data"    class="tab{% if active=='data'   %} active{% endif %}">Data</a>
  <a href="/merge"   class="tab{% if active=='merge'  %} active{% endif %}">Merge</a>
  <a href="/review"  class="tab{% if active=='review' %} active{% endif %}">Review</a>
</div>
"""

# ── Task progress page ──────────────────────────────────────────────────────

TASK_TEMPLATE = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><title>{{ title }}</title>
<style>""" + _CSS + """
body { display:flex; flex-direction:column; min-height:100vh; }
.card { background:#fff; border-radius:8px; padding:32px 40px; margin:40px auto;
        min-width:500px; max-width:700px; box-shadow:0 2px 12px #0002; }
h2   { font-size:16px; margin-bottom:4px; }
.sub { color:#888; font-size:12px; margin-bottom:20px; }
.bar-wrap { background:#e0e0e0; border-radius:6px; height:12px; overflow:hidden; margin-bottom:14px; }
.bar-fill { background:#2a7ae2; height:100%; border-radius:6px; transition:width .3s; width:0; }
.log  { background:#1e1e1e; color:#d4d4d4; font-family:monospace; font-size:11px;
        padding:12px 14px; border-radius:6px; min-height:80px; max-height:300px;
        overflow-y:auto; white-space:pre-wrap; }
.err  { color:#f88; }
.done-btn { margin-top:18px; display:none; }
</style></head><body>
""" + _NAV + """
<div class="card">
  <h2>{{ title }}</h2>
  <div class="sub">{{ subtitle }}</div>
  {% if show_bar %}<div class="bar-wrap"><div class="bar-fill" id="bar"></div></div>{% endif %}
  <div class="log" id="log">Starting…</div>
  <div class="done-btn" id="done">
    <a href="{{ back }}" class="btn btn-primary">✓ Done</a>
  </div>
</div>
<script>
function poll() {
  fetch('/task_status').then(r=>r.json()).then(s=>{
    const log = document.getElementById('log');
    if (s.lines.length) log.textContent = s.lines.join('\\n');
    {% if show_bar %}
    if (s.total > 0)
      document.getElementById('bar').style.width = Math.round(s.iteration/s.total*100)+'%';
    {% endif %}
    if (s.error) {
      log.innerHTML += '\\n<span class="err">Error: '+s.error+'</span>';
      document.getElementById('done').style.display = 'block';
    } else if (!s.running) {
      document.getElementById('done').style.display = 'block';
    } else {
      setTimeout(poll, 500);
    }
  });
}
poll();
</script></body></html>"""

# ── Review tab ──────────────────────────────────────────────────────────────

REVIEW_TEMPLATE = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><title>ZenPal — Review</title>
<style>""" + _CSS + """
.window-bar { background:#1a1a2e; color:#aaa; font-size:12px; padding:7px 20px;
              display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
.toolbar { background:#fff; border-bottom:1px solid #ddd; padding:8px 20px;
           display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
.toolbar label { font-weight:bold; font-size:12px; }
.toolbar input[type=date] { padding:4px 7px; border:1px solid #bbb; border-radius:4px; font-size:12px; }
.count { color:#777; font-size:11px; }
.rev-count { color:#2a7ae2; font-size:11px; font-weight:bold; }
table { width:100%; border-collapse:collapse; }
th { background:#333; color:#fff; padding:6px 8px; text-align:left;
     position:sticky; top:0; z-index:1; font-size:12px; white-space:nowrap; }
tr:nth-child(even) { background:#f9f9f9; }
tr:hover { background:#eef3ff; }
tr.reviewed { background:#e8f5e9 !important; }
tr.reviewed td { opacity:.5; }
td { padding:5px 8px; border-bottom:1px solid #e8e8e8; vertical-align:middle; }
.neg { color:#c00; font-family:monospace; }
.pos { color:#080; font-family:monospace; }
.payee   { font-weight:500; max-width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.comment { color:#999; font-size:11px; max-width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.existing { color:#888; font-style:italic; }
.conf-high { color:#080; font-weight:bold; }
.conf-mid  { color:#960; }
.conf-low  { color:#c00; }
.decision  { display:flex; gap:4px; align-items:center; }
select { padding:3px 5px; border-radius:3px; border:1px solid #bbb; font-size:11px; max-width:160px; }
input.new-tag { padding:3px 5px; border-radius:3px; border:1px solid #bbb; font-size:11px; width:110px; }
input.new-tag:not(:placeholder-shown) + select { opacity:.3; pointer-events:none; }
input.done-cb { width:15px; height:15px; cursor:pointer; accent-color:#2a7ae2; }
</style></head><body>
""" + _NAV + """
{% if message %}<div class="flash">{{ message }}</div>{% endif %}

<div class="window-bar">
  <span>🕐 Train: <b style="color:#fff">{{ window.train_start }} → {{ window.train_end }}</b></span>
  <span>🔍 Predict: <b style="color:#7ecfff">{{ window.predict_start }} → {{ window.predict_end }}</b></span>
  <form method="post" action="/retrain" style="margin:0">
    <button class="btn btn-warn" style="font-size:11px;padding:3px 10px">⚙ Retrain</button>
  </form>
  <form method="post" action="/advance" style="margin:0">
    <button class="btn btn-purple" style="font-size:11px;padding:3px 10px">▶ Advance window</button>
  </form>
</div>

<form method="get" action="/">
<div class="toolbar">
  <label>From</label><input type="date" name="start" value="{{ start }}">
  <label>To</label><input type="date" name="end" value="{{ end }}">
  <button type="submit" class="btn btn-secondary">Load</button>
  <span class="count">{{ transactions|length }} transaction(s)</span>
</div>
</form>

<form method="post" action="/save">
  <input type="hidden" name="start" value="{{ start }}">
  <input type="hidden" name="end"   value="{{ end }}">
  <div class="toolbar">
    <button type="submit" formaction="/save"   class="btn btn-primary">✓ Save CSV</button>
    <button type="submit" formaction="/upload" class="btn btn-success">↑ Upload to Zenmoney</button>
    <button type="button" class="btn btn-secondary" onclick="confirmAll()">Confirm all</button>
    <span class="rev-count"><span id="rc">0</span> / {{ transactions|length }} reviewed</span>
  </div>
<table>
  <thead><tr>
    <th style="width:26px"></th>
    <th>Date</th><th>Amount</th><th>Account</th><th>Payee / Comment</th>
    <th>Existing</th><th>Prediction</th><th>Decision</th>
  </tr></thead>
  <tbody>
  {% for t in transactions %}
  <tr>
    <td style="text-align:center">
      <input class="done-cb" type="checkbox" onchange="markRow(this)">
    </td>
    <td>{{ t.date }}</td>
    <td class="{{ 'neg' if t.amount.startswith('-') else 'pos' }}">{{ t.amount }}</td>
    <td>{{ t.account }}</td>
    <td>
      <div class="payee" title="{{ t.payee }}">{{ t.payee }}</div>
      {% if t.comment %}<div class="comment" title="{{ t.comment }}">{{ t.comment }}</div>{% endif %}
    </td>
    <td class="existing">{{ t.existing_tag or '—' }}</td>
    <td>
      <span class="{{ 'conf-high' if t.conf_value >= 0.7 else ('conf-mid' if t.conf_value >= 0.4 else 'conf-low') }}">
        {{ t.predicted_name }}</span>
      <span style="color:#bbb;font-size:11px"> {{ t.confidence }}</span>
    </td>
    <td>
      <input type="hidden" name="pred_{{ t.id }}" value="{{ t.predicted_tag }}">
      <div class="decision">
        <input class="new-tag" type="text" name="new_{{ t.id }}" placeholder="new…">
        <select name="tag_{{ t.id }}">
          <option value="">— skip —</option>
          {% if t.predicted_tag %}
          <option value="{{ t.predicted_tag }}" selected>{{ t.predicted_name }} ✓</option>
          {% endif %}
          {% for tag in tags %}{% if tag.id != t.predicted_tag %}
          <option value="{{ tag.id }}">{{ tag.title }}</option>
          {% endif %}{% endfor %}
        </select>
      </div>
    </td>
  </tr>
  {% endfor %}
  </tbody>
</table>
</form>
<script>
function markRow(cb) {
  cb.closest('tr').classList.toggle('reviewed', cb.checked);
  document.getElementById('rc').textContent = document.querySelectorAll('.done-cb:checked').length;
}
function confirmAll() {
  document.querySelectorAll('select[name^="tag_"]').forEach(s => {
    for (let i = 0; i < s.options.length; i++)
      if (s.options[i].text.includes('✓')) { s.selectedIndex = i; break; }
  });
  document.querySelectorAll('.done-cb').forEach(cb => {
    cb.checked = true; cb.closest('tr').classList.add('reviewed');
  });
  document.getElementById('rc').textContent = document.querySelectorAll('.done-cb:checked').length;
}
</script></body></html>"""

# ── Data tab ────────────────────────────────────────────────────────────────

DATA_TEMPLATE = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><title>ZenPal — Data</title>
<style>""" + _CSS + """
.content  { max-width:860px; margin:0 auto; padding:24px 20px; }
.section  { background:#fff; border-radius:6px; margin-bottom:18px; box-shadow:0 1px 4px #0001; }
.sec-head { padding:12px 20px; border-bottom:1px solid #eee; font-weight:bold;
            font-size:13px; display:flex; align-items:center; gap:10px; }
.sec-body { padding:16px 20px; }
.hint     { color:#888; font-size:12px; margin-bottom:12px; }
.warn     { color:#a00; font-size:12px; margin-bottom:12px; }
.file-grid { display:grid; grid-template-columns:1fr auto auto; gap:4px 16px;
             font-size:12px; margin-bottom:14px; align-items:center; }
.file-grid .ok  { color:#080; }
.file-grid .bad { color:#c00; }
.prev-acc   { font-weight:bold; font-size:12px; margin:10px 0 4px; }
.prev-table { width:100%; border-collapse:collapse; font-size:11px; margin-bottom:4px; }
.prev-table td { padding:3px 8px; border-bottom:1px solid #f5f5f5; }
.prev-more  { color:#999; font-style:italic; font-size:11px; padding:2px 8px; display:block; }
.push-row   { display:flex; align-items:center; gap:12px; margin-top:14px; }
.push-note  { font-size:12px; color:#555; }
.form-row   { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
.form-row label { font-size:12px; color:#555; }
.form-row input[type=date] { padding:5px 8px; border:1px solid #bbb; border-radius:4px; font-size:12px; }
</style></head><body>
""" + _NAV + """
{% if message %}<div class="flash {{ 'err' if is_err else '' }}">{{ message }}</div>{% endif %}
<div class="content">

  <div class="section">
    <div class="sec-head">⟳ Sync</div>
    <div class="sec-body">
      <p class="hint">Pull latest data from the Zenmoney API and refresh local snapshot.</p>
      <form method="post" action="/sync">
        <button class="btn btn-secondary">Sync now</button>
      </form>
    </div>
  </div>

  <div class="section">
    <div class="sec-head">↓ Import OP Bank CSVs</div>
    <div class="sec-body">
      {% if detected %}
      <div class="file-grid">
        {% for f in detected %}
        <span>{{ f.name }}</span>
        <span style="color:#888">→ {{ f.label }}</span>
        <span class="{{ 'ok' if f.exists else 'bad' }}">{{ '✓' if f.exists else '✗ missing' }}</span>
        {% endfor %}
      </div>
      <button class="btn btn-secondary" id="prev-btn" onclick="doPreview()">Preview new transactions</button>
      <div id="prev-area" style="margin-top:12px"></div>
      <div id="push-row" class="push-row" style="display:none">
        <form method="post" action="/import/push">
          <button class="btn btn-primary">Push &amp; Predict</button>
        </form>
        <span class="push-note" id="push-note"></span>
      </div>
      {% else %}
      <p class="hint">No tapahtumat*.csv files found in {{ downloads_dir }}</p>
      {% endif %}
    </div>
  </div>

  <div class="section">
    <div class="sec-head">↺ Reimport date range</div>
    <div class="sec-body">
      <p class="warn">⚠ Drops all OP transactions in the range, then re-imports from the CSVs above.</p>
      <form method="post" action="/reimport"
            onsubmit="return confirm('Drop and reimport ' + this.start.value + ' → ' + this.end.value + '?')">
        <div class="form-row">
          <label>From</label><input type="date" name="start" required>
          <label>To</label><input type="date" name="end" required>
          <button class="btn btn-danger">Reimport</button>
        </div>
      </form>
    </div>
  </div>

</div>
<script>
function doPreview() {
  const btn = document.getElementById('prev-btn');
  btn.disabled = true; btn.textContent = 'Loading…';
  fetch('/import/detect', {method:'POST'})
    .then(r => r.json())
    .then(data => {
      const area = document.getElementById('prev-area');
      if (data.total === 0) {
        area.innerHTML = '<p style="color:#888;font-size:12px">Nothing new to import.</p>';
      } else {
        let h = '';
        for (const a of data.previews) {
          if (!a.count) continue;
          h += '<div class="prev-acc">' + a.label + ': ' + a.count + ' new</div>';
          h += '<table class="prev-table">';
          for (const t of a.transactions)
            h += '<tr><td style="color:#888;width:90px">' + t.date + '</td>'
               + '<td style="font-family:monospace;color:#c00;width:80px">' + t.amount + '</td>'
               + '<td>' + (t.payee||'') + '</td></tr>';
          if (a.count > a.transactions.length)
            h += '<tr><td colspan="3"><span class="prev-more">… and '+(a.count-a.transactions.length)+' more</span></td></tr>';
          h += '</table>';
        }
        area.innerHTML = h;
        document.getElementById('push-note').textContent = data.total + ' transaction(s) — will predict tags after push';
        document.getElementById('push-row').style.display = 'flex';
      }
      btn.textContent = 'Refresh'; btn.disabled = false;
    })
    .catch(e => {
      document.getElementById('prev-area').innerHTML = '<p style="color:#c00;font-size:12px">Error: '+e+'</p>';
      btn.textContent = 'Preview'; btn.disabled = false;
    });
}
</script></body></html>"""

# ── Merge tab ───────────────────────────────────────────────────────────────

MERGE_TEMPLATE = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><title>ZenPal — Merge</title>
<style>""" + _CSS + """
.content { max-width:760px; margin:0 auto; padding:24px 20px; }
.toolbar { display:flex; align-items:center; gap:12px; margin-bottom:16px; }
.sub-title { font-size:14px; font-weight:bold; }
.count  { color:#777; font-size:12px; }
.pair   { background:#fff; border-radius:6px; margin-bottom:10px; padding:13px 16px;
          box-shadow:0 1px 4px #0001; display:flex; align-items:center; gap:14px; }
.pair-check { width:16px; height:16px; cursor:pointer; accent-color:#2a7ae2; flex-shrink:0; }
.pair-body  { flex:1; }
.prow  { font-size:12px; margin-bottom:2px; font-family:monospace; color:#444; }
.prow .dt   { color:#999; width:88px; display:inline-block; }
.prow .amt  { font-weight:bold; width:80px; display:inline-block; }
.prow .pos  { color:#080; }
.prow .neg  { color:#c00; }
.prow .acc  { color:#333; }
.prow .det  { color:#999; font-size:11px; }
.auto-badge { background:#e8f5e9; color:#1a7a3a; font-size:10px; font-family:sans-serif;
              border-radius:3px; padding:2px 7px; white-space:nowrap; }
.none-msg { color:#999; text-align:center; padding:48px; font-size:13px; }
.footer { margin-top:16px; }
</style></head><body>
""" + _NAV + """
{% if message %}<div class="flash">{{ message }}</div>{% endif %}
<div class="content">
  <div class="toolbar">
    <span class="sub-title">Transfer pairs — last 30 days</span>
    <span class="count">{{ pairs|length }} candidate(s)</span>
    <a href="/merge" class="btn btn-secondary" style="font-size:11px;padding:3px 11px">↺ Refresh</a>
  </div>

  {% if pairs %}
  <form method="post" action="/merge/push">
    <input type="hidden" name="count" value="{{ pairs|length }}">
    {% for p in pairs %}
    <div class="pair">
      <input class="pair-check" type="checkbox" name="pair_{{ loop.index0 }}"
             {% if p.auto %}checked{% endif %}>
      <div class="pair-body">
        <div class="prow">
          <span class="dt">{{ p.date }}</span>
          <span class="amt pos">+{{ p.income }}</span>
          <span class="acc">{{ p.income_account }}</span>
          {% if p.inc_detail %}<span class="det"> — {{ p.inc_detail }}</span>{% endif %}
        </div>
        <div class="prow">
          <span class="dt">{{ p.date }}</span>
          <span class="amt neg">-{{ p.outcome }}</span>
          <span class="acc">{{ p.outcome_account }}</span>
          {% if p.out_detail %}<span class="det"> — {{ p.out_detail }}</span>{% endif %}
        </div>
      </div>
      {% if p.auto %}<span class="auto-badge">auto</span>{% endif %}
      <input type="hidden" name="inc_{{ loop.index0 }}" value="{{ p.inc_id }}">
      <input type="hidden" name="out_{{ loop.index0 }}" value="{{ p.out_id }}">
    </div>
    {% endfor %}
    <div class="footer">
      <button class="btn btn-primary">Merge selected</button>
    </div>
  </form>
  {% else %}
  <div class="none-msg">No transfer candidates found in the last 30 days.</div>
  {% endif %}
</div></body></html>"""


# ---------------------------------------------------------------------------
# Routes — Review
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    return redirect(url_for('review'))

@app.route('/review')
def review():
    w     = load_window()
    start = request.args.get('start', w['predict_start'])
    end   = request.args.get('end',   w['predict_end'])
    return render_template_string(REVIEW_TEMPLATE, active='review',
                                  transactions=build_transactions(start, end),
                                  tags=tags_sorted, start=start, end=end, window=w,
                                  message=request.args.get('message', ''))

@app.route('/save', methods=['POST'])
def save():
    start = request.form.get('start', '')
    end   = request.form.get('end', '')
    rows, _ = parse_form(request.form)
    save_csv(rows)
    return redirect(url_for('review', start=start, end=end,
                            message=f'Saved {len(rows)} row(s) to {OUTPUT_CSV}'))

@app.route('/upload', methods=['POST'])
def upload():
    start = request.form.get('start', '')
    end   = request.form.get('end', '')
    rows, tag_updates = parse_form(request.form)
    save_csv(rows)
    n = sync_tags(tag_updates)
    return redirect(url_for('review', start=start, end=end,
                            message=f'Uploaded {n} tag(s), saved {len(rows)} row(s)'))

@app.route('/advance', methods=['POST'])
def advance():
    w = advance_window(load_window())
    save_window(w)
    return redirect(url_for('review',
                            message=f'Window advanced → {w["predict_start"]} — {w["predict_end"]}'))


# ---------------------------------------------------------------------------
# Routes — Task runner
# ---------------------------------------------------------------------------

@app.route('/task')
def task_page():
    return render_template_string(TASK_TEMPLATE,
                                  active=request.args.get('active', 'review'),
                                  title=request.args.get('title', 'Working…'),
                                  subtitle=request.args.get('subtitle', ''),
                                  back=request.args.get('back', '/'),
                                  show_bar=request.args.get('bar', '0') == '1')

@app.route('/task_status')
def task_status():
    return jsonify(_task)

# Back-compat aliases used by old bookmarks / retrain polling
@app.route('/training')
def training():
    return redirect(url_for('task_page', title='Training model', bar='1', active='review'))

@app.route('/train_status')
def train_status():
    return jsonify(_task)


# ---------------------------------------------------------------------------
# Routes — Retrain
# ---------------------------------------------------------------------------

@app.route('/retrain', methods=['POST'])
def retrain():
    w = load_window()
    _task['total'] = 200

    def _fn():
        def on_progress(i, total):
            _task['iteration'] = i
            _task['total']     = total
        report = train_model(zen, w['train_start'], w['train_end'], progress_fn=on_progress)
        print(report)
        _reload_zen()

    run_task(_fn)
    return redirect(url_for('task_page', title='Training model',
                            subtitle=f'{w["train_start"]} → {w["train_end"]}',
                            bar='1', back='/review', active='review'))


# ---------------------------------------------------------------------------
# Routes — Data
# ---------------------------------------------------------------------------

@app.route('/data')
def data():
    _, slots = detect_op_files(DOWNLOADS)
    detected = [
        {'name': os.path.basename(p), 'label': label, 'exists': os.path.exists(p)}
        for p, _id, label in slots
    ]
    return render_template_string(DATA_TEMPLATE, active='data', detected=detected,
                                  downloads_dir=DOWNLOADS,
                                  message=request.args.get('message', ''),
                                  is_err=request.args.get('err', '0') == '1')

@app.route('/sync', methods=['POST'])
def sync():
    run_task(_reload_zen)
    return redirect(url_for('task_page', title='Syncing',
                            subtitle='Pulling from Zenmoney API',
                            back='/data', active='data'))

@app.route('/import/detect', methods=['POST'])
def import_detect():
    _, slots = detect_op_files(DOWNLOADS)
    previews, total = [], 0
    for path, acc_id, label in slots:
        if not os.path.exists(path):
            previews.append({'label': label, 'count': 0, 'transactions': []})
            continue
        new_txns = get_updates(zen, path, acc_id)['transaction']
        total += len(new_txns)
        previews.append({
            'label': label,
            'count': len(new_txns),
            'transactions': [
                {'date':   t['date'],
                 'amount': f"{'-' if t['outcome'] > 0 else '+'}{t['outcome'] if t['outcome'] > 0 else t['income']:.2f}",
                 'payee':  t.get('originalPayee') or t.get('payee') or ''}
                for t in new_txns[:5]
            ],
        })
    return jsonify({'previews': previews, 'total': total})

@app.route('/import/push', methods=['POST'])
def import_push():
    def _fn():
        _, slots = detect_op_files(DOWNLOADS)
        z = load_or_sync(FILENAME, ZEN_API_TOKEN)
        all_new = []
        for path, acc_id, label in slots:
            if not os.path.exists(path):
                print(f'{label}: skipped (file missing)')
                continue
            new_txns = get_updates(z, path, acc_id)['transaction']
            print(f'{label}: {len(new_txns)} new transaction(s)')
            all_new.extend(new_txns)
        if not all_new:
            print('Nothing to import.')
            _reload_zen()
            return

        new_ids = {t['id'] for t in all_new}
        print(f'Pushing {len(all_new)} transaction(s)...')
        z = load_or_sync(FILENAME, ZEN_API_TOKEN, out_diff={'transaction': all_new})

        if predictor is None:
            print('No model — transactions pushed without tags.')
            _reload_zen()
            return

        print('Running predictions on enriched data...')
        try:
            local_tag_map = {t['id']: t['title'] for t in z.tag}
            enriched = [t for t in z.transaction if t['id'] in new_ids]
            df_new = pd.DataFrame(enriched)
            df_new['date'] = pd.to_datetime(df_new['date'])
            df_tagged = predictor.tag(df_new, z)

            tag_updates = []
            for row in df_tagged.to_dict('records'):
                tag_id = row.get('final_tag')
                tag_name = local_tag_map.get(tag_id, '(no tag)') if tag_id else '(no tag)'
                payee = row.get('originalPayee') or row.get('payee') or ''
                amt = row.get('outcome', 0) or 0
                amt_s = f"-{amt:.2f}" if amt else f"+{row.get('income', 0):.2f}"
                date_s = pd.Timestamp(row['date']).strftime('%Y-%m-%d')
                print(f"  {date_s}  {amt_s:>10}  {payee[:38]:<38}  → {tag_name}")
                if tag_id:
                    tag_updates.extend(z.set_tags(row['id'], [tag_id])['transaction'])

            if tag_updates:
                load_or_sync(FILENAME, ZEN_API_TOKEN, out_diff={'transaction': tag_updates})
                print(f'Applied tags to {len(tag_updates)}/{len(all_new)} transaction(s).')
            else:
                print('Predictions returned no tags.')
        except Exception as e:
            print(f'Warning: prediction failed ({e}), transactions pushed without tags.')
        _reload_zen()

    run_task(_fn)
    return redirect(url_for('task_page', title='Importing',
                            subtitle='OP Bank transactions', back='/data', active='data'))

@app.route('/reimport', methods=['POST'])
def reimport():
    start = request.form['start']
    end   = request.form['end']
    op_ids = {acc_id for acc_id, _ in ACCOUNT_SLOTS}

    def _fn():
        from config import ZEN_USER as _user
        import uuid as _uuid

        _, slots = detect_op_files(DOWNLOADS)
        z = load_or_sync(FILENAME, ZEN_API_TOKEN)

        to_drop = [
            t for t in z.transaction
            if not t.get('deleted')
            and start <= t['date'] <= end
            and (t.get('incomeAccount') in op_ids or t.get('outcomeAccount') in op_ids)
            and not (t['income'] == 0 and t['outcome'] == 0)
        ]

        if to_drop:
            now = int(time.time())
            diff_txns = []
            for t in to_drop:
                diff_txns.append({**t, 'deleted': True, 'changed': now})
                if t['income'] > 0 and t['outcome'] > 0 and t.get('incomeAccount') not in op_ids:
                    print(f'  restoring non-OP income: +{t["income"]} on {t["date"]}')
                    diff_txns.append({
                        'id': str(_uuid.uuid4()), 'user': _user,
                        'date': t['date'], 'income': t['income'], 'outcome': 0,
                        'incomeAccount': t['incomeAccount'], 'outcomeAccount': t['incomeAccount'],
                        'incomeInstrument': t['incomeInstrument'], 'outcomeInstrument': t['incomeInstrument'],
                        'changed': now, 'created': now, 'deleted': False,
                        'tag': [], 'comment': t.get('comment'), 'payee': t.get('payee'),
                        'originalPayee': t.get('originalPayee'), 'merchant': None,
                        'reminderMarker': None, 'incomeBankID': None, 'outcomeBankID': None,
                        'opIncome': None, 'opOutcome': None,
                        'opIncomeInstrument': None, 'opOutcomeInstrument': None,
                        'latitude': None, 'longitude': None,
                    })
            print(f'Dropping {len(to_drop)} transaction(s)...')
            load_or_sync(FILENAME, ZEN_API_TOKEN, out_diff={'transaction': diff_txns})

        z = load_or_sync(FILENAME, ZEN_API_TOKEN)
        for path, acc_id, label in slots:
            if not os.path.exists(path):
                print(f'Skipped (not found): {path}')
                continue
            print(f'Importing {label}...')
            diff = get_updates(z, path, acc_id)
            print(f'  {len(diff["transaction"])} new transaction(s)')
            if diff['transaction']:
                load_or_sync(FILENAME, ZEN_API_TOKEN, out_diff=diff)
        _reload_zen()

    run_task(_fn)
    return redirect(url_for('task_page', title='Reimporting',
                            subtitle=f'{start} → {end}', back='/data', active='data'))


# ---------------------------------------------------------------------------
# Routes — Merge
# ---------------------------------------------------------------------------

@app.route('/merge')
def merge():
    from datetime import date, timedelta
    since      = (date.today() - timedelta(days=30)).isoformat()
    candidates = find_candidates(zen.transaction, start_date=since)

    pairs = []
    for inc, out in candidates:
        auto = any(rule(inc, out) for rule in MERGE_RULES)
        pairs.append({
            'date':           inc['date'],
            'income':         f"{inc['income']:.2f}",
            'outcome':        f"{out['outcome']:.2f}",
            'income_account': account_map.get(inc['incomeAccount'],  inc['incomeAccount']),
            'outcome_account':account_map.get(out['outcomeAccount'], out['outcomeAccount']),
            'inc_detail': '  '.join(filter(None, [inc.get('originalPayee') or inc.get('payee'), inc.get('comment')])),
            'out_detail': '  '.join(filter(None, [out.get('originalPayee') or out.get('payee'), out.get('comment')])),
            'auto':    auto,
            'inc_id':  inc['id'],
            'out_id':  out['id'],
        })

    return render_template_string(MERGE_TEMPLATE, active='merge', pairs=pairs,
                                  message=request.args.get('message', ''))

@app.route('/merge/push', methods=['POST'])
def merge_push():
    count    = int(request.form.get('count', 0))
    txn_map  = {t['id']: t for t in zen.transaction}
    confirmed = []
    for i in range(count):
        if request.form.get(f'pair_{i}'):
            inc = txn_map.get(request.form.get(f'inc_{i}'))
            out = txn_map.get(request.form.get(f'out_{i}'))
            if inc and out:
                confirmed.append((inc, out))

    if not confirmed:
        return redirect(url_for('merge', message='Nothing selected.'))

    now  = int(time.time())
    diff = {'transaction': []}
    for inc, out in confirmed:
        diff['transaction'].append(_build_transfer(inc, out))
        diff['transaction'].append({**inc, 'deleted': True, 'changed': now})
        diff['transaction'].append({**out, 'deleted': True, 'changed': now})

    def _fn():
        load_or_sync(FILENAME, ZEN_API_TOKEN, out_diff=diff)
        _reload_zen()
        print(f'Merged {len(confirmed)} pair(s).')

    run_task(_fn)
    return redirect(url_for('task_page', title='Merging transfers',
                            subtitle=f'{len(confirmed)} pair(s)',
                            back='/merge', active='merge'))


# ---------------------------------------------------------------------------
# Routes — Shutdown
# ---------------------------------------------------------------------------

@app.route('/shutdown', methods=['POST'])
def shutdown():
    threading.Timer(0.3, lambda: os._exit(0)).start()
    return '<h2 style="font-family:sans-serif;padding:48px;color:#555">Server stopped. You can close this tab.</h2>'


# ---------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=False, port=5000)
