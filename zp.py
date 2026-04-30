#!/usr/bin/env python3
"""ZenPal CLI  —  python zp.py <command> [options]"""
import argparse
import os
import time
import threading
import uuid
import webbrowser

from config import ZEN_API_TOKEN, ZEN_USER
import zenmoney
from zenmoney.op import OPReader

FILENAME     = 'zenmoney.json'
DOWNLOADS    = r'C:\Users\eltha\Downloads'
PREVIEW_ROWS = 5

OP_EUR_ID    = 'f02f21c3-2686-4c78-a3da-cc4c776fba93'
OP_FAMILY_ID = '50ca9746-f13a-4b67-adbb-1fe8f7f28439'
OP_MONEY_BOX = '052b718d-74f1-4e25-b1fa-5f7b9e7a7ca4'

# Ordered to match OP Bank's export suffix convention: base, (1), (2)
ACCOUNT_SLOTS = [
    (OP_EUR_ID,    'OP EUR'),
    (OP_FAMILY_ID, 'OP Family'),
    (OP_MONEY_BOX, 'OP Money Box'),
]


# ---------------------------------------------------------------------------
# Core sync / import
# ---------------------------------------------------------------------------

def load_or_sync(filename=FILENAME, token=ZEN_API_TOKEN, out_diff=None):
    conn = zenmoney.ZenConnection(token)
    if os.path.exists(filename):
        print('Syncing...')
        zen = zenmoney.Zenmoney.load(filename)
        conn.sync_timestamp = zen.server_timestamp
        diff = conn.sync(diff=out_diff)
        zen.apply_diff(diff)
        zen.write(filename)
        print('Sync done.')
    else:
        print('Getting initial data...')
        zen = zenmoney.Zenmoney(conn.sync())
        zen.write(filename)
        print('Done.')
    return zen


def get_updates(zen, csv_file, acc_id):
    def check(zen_txn, op_txn):
        fields = ['date']
        if op_txn['income'] > 0:
            fields += ['income', 'incomeAccount']
        elif op_txn['outcome'] > 0:
            fields += ['outcome', 'outcomeAccount']
        for f in fields:
            if zen_txn.get(f) != op_txn.get(f):
                return False
        return True

    diff = {'transaction': []}
    op = OPReader(filename=csv_file, zen_id=acc_id, instrument_id=3)
    for line in op.read():
        if line['income'] == 0 and line['outcome'] == 0:
            continue
        if not any(check(x, line) for x in zen.transaction if not x.get('deleted')):
            line.update({
                'id': str(uuid.uuid4()),
                'created': line['changed'],
                'user': ZEN_USER,
                'deleted': False,
                'tag': [],
                'merchant': None,
                'reminderMarker': None,
                'incomeBankID': None,
                'outcomeBankID': None,
                'opIncome': None,
                'opOutcome': None,
                'opIncomeInstrument': None,
                'opOutcomeInstrument': None,
                'latitude': None,
                'longitude': None,
            })
            diff['transaction'].append(line)
    return diff


# ---------------------------------------------------------------------------
# Import helpers
# ---------------------------------------------------------------------------


def detect_op_files(downloads_dir):
    """Return (date_suffix, [(path, acc_id, label), ...]) for the latest tapahtumat group."""
    try:
        files = os.listdir(downloads_dir)
    except FileNotFoundError:
        return None, []
    base_files = sorted(
        [f for f in files if f.startswith('tapahtumat') and f.endswith('.csv') and '(' not in f],
        reverse=True,
    )
    if not base_files:
        return None, []
    date_suffix = base_files[0][len('tapahtumat'):-len('.csv')]
    slots = [
        (os.path.join(downloads_dir, f'tapahtumat{date_suffix}{s}.csv'), acc_id, label)
        for (acc_id, label), s in zip(ACCOUNT_SLOTS, ['', ' (1)', ' (2)'])
    ]
    return date_suffix, slots


def _fmt(txn):
    return f"{'-' if txn['outcome'] > 0 else '+'}{txn['outcome'] if txn['outcome'] > 0 else txn['income']:.2f}"


def _preview(new_txns):
    for t in new_txns[:PREVIEW_ROWS]:
        payee = t.get('originalPayee') or t.get('payee') or ''
        print(f"    {t['date']}  {_fmt(t):>10}  {payee}")
    if len(new_txns) > PREVIEW_ROWS:
        print(f"    ... and {len(new_txns) - PREVIEW_ROWS} more")


def _confirm(prompt):
    return input(prompt).strip().lower() in ('', 'y')


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------

MERGE_RULES = [
    lambda inc, out: bool(inc.get('comment')) and inc.get('comment') == out.get('comment'),
    lambda inc, out: any('LT053250022295729145' in (t.get('comment') or '') for t in (inc, out)),
]


def find_candidates(transactions, start_date=None):
    active = [t for t in transactions if not t.get('deleted')]
    if start_date:
        active = [t for t in active if t['date'] >= start_date]
    incomes  = [t for t in active if t['income'] > 0 and t['outcome'] == 0]
    outcomes = [t for t in active if t['outcome'] > 0 and t['income'] == 0]
    used_inc, used_out = set(), set()
    pairs = []
    for inc in incomes:
        for out in outcomes:
            if inc['id'] in used_inc or out['id'] in used_out:
                continue
            if (inc['date'] == out['date']
                    and inc['income'] == out['outcome']
                    and inc['incomeInstrument'] == out['outcomeInstrument']
                    and inc['incomeAccount'] != out['outcomeAccount']):
                pairs.append((inc, out))
                used_inc.add(inc['id'])
                used_out.add(out['id'])
    return pairs


def _build_transfer(inc, out):
    now = int(time.time())
    return {
        'id': str(uuid.uuid4()), 'user': ZEN_USER,
        'date': inc['date'], 'income': inc['income'], 'outcome': out['outcome'],
        'incomeAccount': inc['incomeAccount'], 'outcomeAccount': out['outcomeAccount'],
        'incomeInstrument': inc['incomeInstrument'], 'outcomeInstrument': out['outcomeInstrument'],
        'changed': now, 'created': now, 'deleted': False,
        'tag': [], 'comment': None, 'payee': None, 'originalPayee': None,
        'merchant': None, 'reminderMarker': None,
        'incomeBankID': None, 'outcomeBankID': None,
        'opIncome': None, 'opOutcome': None,
        'opIncomeInstrument': None, 'opOutcomeInstrument': None,
        'latitude': None, 'longitude': None,
    }


def _describe(txn, account_map):
    if txn['income'] > 0 and txn['outcome'] == 0:
        line = f"+{txn['income']} → {account_map.get(txn['incomeAccount'], txn['incomeAccount'])}"
    else:
        line = f"-{txn['outcome']} ← {account_map.get(txn['outcomeAccount'], txn['outcomeAccount'])}"
    extra = '  '.join(filter(None, [txn.get('originalPayee') or txn.get('payee') or '', txn.get('comment') or '']))
    return f"[{txn['date']}] {line}  {extra}"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_sync(_args):
    load_or_sync()


def cmd_import(args):
    date_suffix, slots = detect_op_files(args.dir)
    if not slots:
        print(f'No tapahtumat*.csv files found in {args.dir}')
        return

    print(f'\nDetected files (date range: {date_suffix}):')
    for path, _acc_id, label in slots:
        status = 'ok' if os.path.exists(path) else 'missing'
        print(f'  {os.path.basename(path):<46} → {label}  [{status}]')

    if not args.yes and not _confirm('\nConfirm mapping? [Y/n] '):
        print('Aborted.')
        return

    zen = load_or_sync()
    print()

    all_new = []
    for path, acc_id, label in slots:
        if not os.path.exists(path):
            print(f'  {label}: skipped (file missing)')
            continue
        diff = get_updates(zen, path, acc_id)
        new_txns = diff['transaction']
        if new_txns:
            print(f'  {label}: {len(new_txns)} new transaction(s)')
            _preview(new_txns)
        else:
            print(f'  {label}: nothing new')
        all_new.extend(new_txns)

    if not all_new:
        print('\nNothing to import.')
        return

    if not args.yes and not _confirm(f'\nPush {len(all_new)} transaction(s) to Zenmoney? [Y/n] '):
        print('Aborted.')
        return

    new_ids = {t['id'] for t in all_new}
    print('Pushing raw transactions...')
    zen = load_or_sync(out_diff={'transaction': all_new})

    try:
        import pandas as pd
        from prediction.pipeline import Predictor
        predictor = Predictor()
        local_tag_map = {t['id']: t['title'] for t in zen.tag}
        enriched = [t for t in zen.transaction if t['id'] in new_ids]
        df = pd.DataFrame(enriched)
        df['date'] = pd.to_datetime(df['date'])
        df_tagged = predictor.tag(df, zen)

        tag_updates = []
        print()
        for row in df_tagged.to_dict('records'):
            tag_id = row.get('final_tag')
            tag_name = local_tag_map.get(tag_id, '(no tag)') if tag_id else '(no tag)'
            payee = row.get('originalPayee') or row.get('payee') or ''
            amt = row.get('outcome', 0) or 0
            amt_s = f"-{amt:.2f}" if amt else f"+{row.get('income', 0):.2f}"
            date_s = pd.Timestamp(row['date']).strftime('%Y-%m-%d')
            print(f"  {date_s}  {amt_s:>10}  {payee[:38]:<38}  → {tag_name}")
            if tag_id:
                tag_updates.extend(zen.set_tags(row['id'], [tag_id])['transaction'])

        if tag_updates:
            print()
            load_or_sync(out_diff={'transaction': tag_updates})
            print(f'Applied tags to {len(tag_updates)}/{len(all_new)} transaction(s).')
        else:
            print('\nNo tags to apply.')
    except Exception as e:
        print(f'Warning: prediction failed ({e}), transactions pushed without tags.')


def cmd_reimport(args):
    op_ids = {acc_id for acc_id, _ in ACCOUNT_SLOTS}
    _, slots = detect_op_files(args.dir)
    if not slots:
        print(f'No tapahtumat*.csv files found in {args.dir}')
        return

    print(f'Reimporting {args.start} → {args.end}')
    zen = load_or_sync()

    to_drop = [
        t for t in zen.transaction
        if not t.get('deleted')
        and args.start <= t['date'] <= args.end
        and (t.get('incomeAccount') in op_ids or t.get('outcomeAccount') in op_ids)
        and not (t['income'] == 0 and t['outcome'] == 0)
    ]

    if not to_drop:
        print('No transactions to drop in range.')
    else:
        print(f'Dropping {len(to_drop)} transaction(s)...')
        now = int(time.time())
        diff_txns = []
        for t in to_drop:
            diff_txns.append({**t, 'deleted': True, 'changed': now})
            if (t['income'] > 0 and t['outcome'] > 0 and t.get('incomeAccount') not in op_ids):
                print(f'  restoring non-OP income: +{t["income"]} on {t["date"]}')
                diff_txns.append({
                    'id': str(uuid.uuid4()), 'user': ZEN_USER,
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
        load_or_sync(out_diff={'transaction': diff_txns})
        print('Drop synced.')

    zen = load_or_sync()
    for path, acc_id, label in slots:
        if not os.path.exists(path):
            print(f'Skipped (not found): {path}')
            continue
        print(f'Importing {label}...')
        diff = get_updates(zen, path, acc_id)
        print(f'  {len(diff["transaction"])} new transaction(s)')
        if diff['transaction']:
            load_or_sync(out_diff=diff)


def cmd_merge(args):
    from datetime import date, timedelta
    since = args.since or (date.today() - timedelta(days=30)).isoformat()
    zen = load_or_sync()
    print(f'Looking for transfer pairs since {since}')
    candidates = find_candidates(zen.transaction, start_date=since)
    if not candidates:
        print('No transfer candidates found.')
        return

    account_map = {a['id']: a['title'] for a in zen.account}
    print(f'\nFound {len(candidates)} candidate pair(s):\n')
    confirmed = []
    for i, (inc, out) in enumerate(candidates):
        auto = any(rule(inc, out) for rule in MERGE_RULES)
        print(f'Pair {i + 1}:')
        print(f'  income:  {_describe(inc, account_map)}')
        print(f'  outcome: {_describe(out, account_map)}')
        if args.yes or auto:
            print(f'  Merge? {"[auto]" if auto else "[--yes]"}')
            confirmed.append((inc, out))
        else:
            if input('  Merge? [y/N] ').strip().lower() == 'y':
                confirmed.append((inc, out))
        print()

    if not confirmed:
        print('Nothing to merge.')
        return

    now = int(time.time())
    diff = {'transaction': []}
    for inc, out in confirmed:
        diff['transaction'].append(_build_transfer(inc, out))
        diff['transaction'].append({**inc, 'deleted': True, 'changed': now})
        diff['transaction'].append({**out, 'deleted': True, 'changed': now})

    print(f'Syncing {len(confirmed)} merge(s)...')
    load_or_sync(out_diff=diff)
    print('Done.')


def cmd_serve(args):
    import web_server
    url = f'http://localhost:{args.port}'
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    print(f'Serving at {url}  (Ctrl+C to stop)')
    web_server.app.run(debug=False, port=args.port)


def cmd_zen(_args):
    webbrowser.open('https://zenmoney.ru/a/#')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(prog='zp', description='ZenPal CLI')
    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('sync',
                   help='Sync zenmoney.json with the API')

    p = sub.add_parser('import',
                       help='Detect OP Bank CSVs in Downloads, preview, and push')
    p.add_argument('--dir', default=DOWNLOADS, metavar='DIR')
    p.add_argument('-y', '--yes', action='store_true', help='Skip confirmation prompts')

    p = sub.add_parser('reimport',
                       help='Drop a date range from OP accounts and re-import from CSVs')
    p.add_argument('--start', required=True, metavar='YYYY-MM-DD')
    p.add_argument('--end',   required=True, metavar='YYYY-MM-DD')
    p.add_argument('--dir', default=DOWNLOADS, metavar='DIR')

    p = sub.add_parser('merge',
                       help='Find and merge transfer pairs')
    p.add_argument('--since', default=None, metavar='YYYY-MM-DD',
                   help='Only consider transactions on or after this date (default: 30 days ago)')
    p.add_argument('-y', '--yes', action='store_true', help='Auto-confirm all pairs')

    p = sub.add_parser('serve',
                       help='Start the web review interface and open in browser')
    p.add_argument('--port', type=int, default=5000)

    sub.add_parser('zen',
                   help='Open Zenmoney in browser')

    args = parser.parse_args()
    {
        'sync':     cmd_sync,
        'import':   cmd_import,
        'reimport': cmd_reimport,
        'merge':    cmd_merge,
        'serve':    cmd_serve,
        'zen':      cmd_zen,
    }[args.command](args)


if __name__ == '__main__':
    main()
