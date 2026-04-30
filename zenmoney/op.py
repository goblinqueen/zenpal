class OPReader:
    def __init__(self, filename, zen_id, instrument_id):
        self._filename = filename
        self._zen_id = zen_id
        self._instrument_id = instrument_id

    def read(self):
        import csv
        import time

        read_ts = int(time.time())

        def get_date(x):
            if 'OSTOPVM' in x:
                dt = x.split('OSTOPVM ')[1].split('MF')[0]
                return f"20{dt[0:2]}-{dt[2:4]}-{dt[4:6]}"
            return None

        with open(self._filename, encoding='utf-8') as f:
            title = None
            for row in csv.reader(f, delimiter=';'):
                if not title:
                    title = row
                else:
                    row = dict(zip(title, row))
                    amount = float(row['Määrä EUROA'].replace(",", "."))

                    purchase_date = get_date(row['Viesti'])
                    if not purchase_date:
                        purchase_date = row['Arvopäivä']
                    yield {
                        'date': purchase_date,
                        'income': amount if amount > 0 else 0,
                        'outcome': -1 * amount if amount < 0 else 0,
                        'incomeInstrument': self._instrument_id,
                        'outcomeInstrument': self._instrument_id,
                        'payee': row['Saaja/Maksaja'] + ' ' + row['Saajan tilinumero'],
                        'comment': row['Viesti'],
                        'incomeAccount': self._zen_id,
                        'outcomeAccount': self._zen_id,
                        'changed': read_ts
                    }