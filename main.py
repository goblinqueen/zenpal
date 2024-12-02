import os.path
import uuid
from config import *

OP_FAMILY_ID = '50ca9746-f13a-4b67-adbb-1fe8f7f28439'
OP_EUR_ID = 'f02f21c3-2686-4c78-a3da-cc4c776fba93'
OP_MONEY_BOX = '052b718d-74f1-4e25-b1fa-5f7b9e7a7ca4'

from op import OPReader
import zenmoney

def load_or_sync(filename, token, out_diff=None):
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


def get_updates(zen, _filename, _acc_id):
    diff = {'transaction': []}

    op = OPReader(
        filename=_filename,
        zen_id=_acc_id,
        instrument_id=3)
    for line in op.read():
        def check(_zen, _op):


            _f = ['date']  # Fields to compare (all must match)
            if _op['income'] > 0:
                _f += ['income']
                _f += ['incomeAccount']
            else:
                _f += ['outcome']
                _f += ['outcomeAccount']

            for _n in _f:
                if not _zen.get(_n, None) == _op.get(_n, None):
                    return False

            if _zen.get('deleted'):
                print(_op)
            return True

        search = [x for x in zen.transaction if check(x, line)]

        if len(search) == 0:
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



def main():
    token = ZEN_API_TOKEN
    filename = 'zenmoney.json'

    zen = load_or_sync(filename, token)
    in_dates = '20240901-20241126'

    op_files = [
        # (f'/Users/eltha/Downloads/tapahtumat{in_dates}.csv', OP_EUR_ID),
        (f'/Users/eltha/Downloads/tapahtumat{in_dates} (1).csv', OP_FAMILY_ID),
        # (f'/Users/eltha/Downloads/tapahtumat{in_dates} (2).csv', OP_MONEY_BOX),
    ]

    # print([x for x in zen.account if 'OP M' in x['title']])

    for op_file in op_files:
        print()
        print(zen.get_by_value('account', 'id', op_file[1])[0]['title'])
        diff = get_updates(zen, op_file[0], op_file[1])
        print(f'Updating {len(diff["transaction"])} items')
        load_or_sync(filename, token, out_diff=diff)

if __name__ == '__main__':
    main()
