import csv
import datetime
import sys

ACCOUNT_CURR = 'USD'
CONVERSION = 'General Currency Conversion'

DT, AMT = 1, 0

Currency = 'Currency'
Amount = 'Amount'
Date = '\ufeff"Date"'
Time = 'Time'
Type = 'Type'
Name = 'Name'


def to_unixtime(rrow):
    return int(datetime.datetime.strptime(rrow[Date] + ' ' + rrow[Time], "%d/%m/%Y %H:%M:%S").timestamp())


def to_isodate(s):
    return str(datetime.datetime.strptime(s, "%d/%m/%Y"))



def load(filename):
    with open(filename) as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        header = {}
        out_lines = []
        conv = {}
        pending_trans = []
        pending_conv = {}

        for row in csv_reader:
            # if line_count > 40:
            #     break
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
                    # a=1
                    out_lines.append(['TRAN', to_isodate(row[Date]), row[Name], row[Type], row[Amount]])
                else:
                    pending_trans.append(row)

            line_count += 1

    for row in pending_trans:
        if row[Amount] in conv.get(row[Currency], {}):
            conv_lists = conv[row[Currency]][row[Amount]]
            if len(conv_lists) == 1:
                conv_list = conv_lists[0]
            else:
                tr_time = to_unixtime(row)
                conv_list = conv_lists[0]
                curr_diff = abs(conv_list[DT] - tr_time)
                for x in conv_lists:
                    if abs(x[DT] - tr_time) < curr_diff:
                        curr_diff = abs(x[DT] - tr_time)
                        conv_list = x
            out_lines.append(['TRAN_', to_isodate(row[Date]), row[Name], row[Type], conv_list[AMT], row[Currency] + " " + row[Amount]])
        # else:
        #     out_lines.append(
        #         ['TRAN_', to_isodate(row[Date]), row[Name], row[Type], 'NaN', row[Currency] + " " + row[Amount]])

    return out_lines


if __name__ == "__main__":
    for line in load('Download.CSV'):
        # a=0
        print("\t".join(line))
