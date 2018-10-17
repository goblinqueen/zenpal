import csv
import datetime
import sys
import forex_python.converter
from argparse import ArgumentParser

ACCOUNT_CURR = 'USD'
CONVERSION = 'General Currency Conversion'

DT, AMT = 1, 0

Currency = 'Currency'
Amount = 'Amount'
Date = 'Date'
Time = 'Time'
Type = 'Type'
Name = 'Name'


def to_unixtime(rrow):
    return int(datetime.datetime.strptime(rrow[Date] + ' ' + rrow[Time], "%d/%m/%Y %H:%M:%S").timestamp())


def to_isodate(s):
    return str(datetime.datetime.strptime(s, "%d/%m/%Y"))


def convert_cb(amount, curr, dt):
    converter = forex_python.converter.CurrencyRates()
    dt = datetime.datetime.utcfromtimestamp(dt)
    rate = converter.get_rate(curr, ACCOUNT_CURR, dt)
    return str(rate * float(amount))  # Difference with PayPal rate ranges from 0.95 to 1.12, it's the same on average.


def load(filename):
    def pre_process(data):
        for row in data:
            yield row.replace('\ufeff', '')

    with open(filename) as csv_file:
        csv_reader = csv.reader(pre_process(csv_file), delimiter=',')
        line_count = 0
        header = {}
        out_lines = []
        conv = {}
        pending_trans = []
        pending_conv = {}

        for row in csv_reader:
            if not header:
                header = row
                continue
            row = dict(zip(header, row))
            row[Amount] = row[Amount].replace(",", "")
            if row[Type] == CONVERSION:
                dkey = to_unixtime(row)

                pending_conv[dkey] = pending_conv.get(dkey, {})
                pending_conv[dkey][row[Currency]] = row[Amount]
                if len(pending_conv[dkey]) == 2:
                    if ACCOUNT_CURR not in pending_conv[dkey]:
                        print("WARN: unparseable conversion {}".format(pending_conv[dkey]), file=sys.stderr)
                    else:
                        curr = [x for x in pending_conv[dkey].keys() if x != ACCOUNT_CURR][0]
                        usd_amount = pending_conv[dkey][ACCOUNT_CURR]
                        rub_amount = pending_conv[dkey][curr].replace("-", "")
                        conv[curr] = conv.get(curr, {})
                        t = conv[curr].get(rub_amount, [])
                        t.append([usd_amount, dkey])
                        conv[curr][rub_amount] = t

            else:
                if row[Currency] == ACCOUNT_CURR:
                    out_lines.append([to_isodate(row[Date]), row[Name], row[Type], row[Amount]])
                else:
                    pending_trans.append(row)

            line_count += 1

    for row in pending_trans:
        abs_amount = row[Amount].replace("-", "")
        usd_amount = None
        if abs_amount in conv.get(row[Currency], {}):
            conv_lists = conv[row[Currency]][abs_amount]
            tr_time = to_unixtime(row)
            if len(conv_lists) == 1:
                conv_list = conv_lists[0]
            else:
                conv_list = conv_lists[0]
                curr_diff = abs(conv_list[DT] - tr_time)
                for x in conv_lists:
                    if abs(x[DT] - tr_time) < curr_diff:
                        curr_diff = abs(x[DT] - tr_time)
                        conv_list = x
            if abs(conv_list[DT] - tr_time) <= 60 * 60 * 24:
                usd_amount = conv_list[AMT]
        if not usd_amount:
            print("WARN: fetching {} {} from forex".format(row[Amount], row[Currency]), file=sys.stderr)
            usd_amount = convert_cb(row[Amount], row[Currency], to_unixtime(row))
        out_lines.append([to_isodate(row[Date]), row[Name], row[Type] + " (" + row[Amount] + " " + row[Currency] + ")",
                          usd_amount])

    return out_lines


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-f', '--file', required=True, help='Path or name of the file you wish to edit')
    parser.add_argument('-o', '--output_file', required=False, help='Path or name of the desired output file. Optional')
    parser.add_argument('-a', '--append', type=bool, required=False,
                        help="'True' will add to existing file\n 'False' will create a new output file")

    args = parser.parse_args()
    infile = args.file
    outfile = args.output_file
    output_writer = csv.writer(sys.stdout, delimiter=';', quoting=csv.QUOTE_MINIMAL)

    try:
        if args.output_file:
            if args.append:
                with open(outfile, 'at', newline='') as f:
                    file_writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                    for line in load(infile):
                        file_writer.writerow(line)
            else:
                with open(outfile, 'wt', newline='') as f:
                    file_writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                    for line in load(infile):
                        file_writer.writerow(line)
        else:
            for line in load(infile):
                output_writer.writerow(line)
    except FileNotFoundError:
        parser.error("{} file or path does not exist please try again.".format(args.file))
