from v8log import v8log
from argparse import ArgumentParser
from datetime import datetime

parser = ArgumentParser(description='Sample script using v8log module')
parser.add_argument('db_file')
parser.add_argument('-d, -date', nargs='?', default='',
                    help='Date in YYYY-MM-DD formst. If no date given, clean up event log',
                    dest='to_date')
parser.add_argument('-v, -vacuum', action='store_true', default=False,
                    help='If specified, database mist be closed first', dest='vacuum')
args = parser.parse_args()
log = v8log(args.db_file)
if args.to_date == '':
    log.truncate_log()
else:
    log.truncate_log(datetime.strptime(args.to_date, '%Y-%m-%d'))
if args.vacuum:
    log.vacuum()
