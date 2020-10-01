# zenpal
PayPal statement to zenmoney csv import converter

# usage

PayPay statement can be acquired here: https://business.paypal.com/merchantdata/consumerHome
(Format: CSV)

Zenmoney csv import interface is here: https://zenmoney.ru/a/#import

Usage example:

    $ ./zenpal.py -f Download.csv -o Converted.csv -a

This will parse the Download.csv file and append the converted content to Converted.csv

    optional arguments:
        -h, --help            show this help message and exit
        -f FILE, --file FILE  Path or name of the file you wish to edit
        -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        Path or name of the desired output file. Optional
        -a APPEND, --append APPEND
                        'True' will add to existing file 'False' will create a
                        new output file


# known issues

    forex_python.converter.RatesNotAvailableError: Currency Rates Source Not Ready

Caused by: https://github.com/MicroPyramid/forex-python/issues/65

Updating forex-python directly from git usually helps:

    pip install git+https://github.com/MicroPyramid/forex-python.git
