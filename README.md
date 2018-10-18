# zenpal
PayPal statement to zenmoney csv import converter

# usage

PayPay statement can be acquired here: https://business.paypal.com/merchantdata/consumerHome
(Format: CSV)

Zenmoney csv import interface is here: https://zenmoney.ru/a/#import

The file called Download.csv can be placed anywhere (take note of the file location). Then run from a command line:

    $ python3 zenpal.py -f ./Download.csv -o Converted.csv -a True
	
This will parse the Download.csv file and append the converted content to Converted.csv
    
    optional arguments:
        -h, --help            show this help message and exit
        -f FILE, --file FILE  Path or name of the file you wish to edit
        -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        Path or name of the desired output file. Optional
        -a APPEND, --append APPEND
                        'True' will add to existing file 'False' will create a
                        new output file