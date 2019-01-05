from sqlite3 import connect
from datetime import datetime, timedelta


class v8log:
    def __init__(self, db_path):
        self.connection = connect(db_path)
        self.execute_query('pragma synchronous = 0')
        self.execute_query('pragma journal_mode = truncate')

    def __del__(self):
        self.connection.close()

    def get_min_date(self):
        return v8log.logdate_to_datetime(self.execute_query("select ifnull(min(date), 0) from  eventlog").fetchone()[0])

    def get_max_date(self):
        return v8log.logdate_to_datetime(self.execute_query("select ifnull(max(date), 0) from  eventlog").fetchone()[0])

    def execute_query(self, query_text, parameters=None, commit=True):
        if parameters is None:
            res = self.connection.execute(query_text)
        else:
            res = self.connection.execute(query_text, parameters)
        if commit:
            self.connection.commit()
        return res

    def truncate_log(self, to_date=datetime.min):
        # 1.when no end date specified, delete all eventlog
        if to_date == datetime.min:
            self.execute_query('delete from eventlog')
            return
        # 2.if no data in eventlog (return 0 as max date) or to_data behind the min_date, nothing to do
        min_date = self.get_min_date()
        if min_date == datetime.min or min_date > to_date:
            return
        # 3.anything else. delete by 3 day chunks - it is faster and less disk usage
        chunk_size = 3
        min_date -= timedelta(1)  # to aviod rounding
        max_date = min(to_date, min_date + timedelta(chunk_size))
        while min_date < max_date:
            # print('processing ', max_date.date())
            self.execute_query('delete from eventlog where date between ? and ?',
                               (v8log.datetime_to_logdate(min_date), v8log.datetime_to_logdate(max_date)))
            min_date = max_date
            max_date = min(max_date + timedelta(chunk_size), to_date)

    def vacuum(self):
        self.connection.executescript('vacuum;')

    @classmethod
    def datetime_to_logdate(cls, date):
        return 0 if date == datetime.min else int(date.timestamp() * 10000) + 621355788000000

    @classmethod
    def logdate_to_datetime(cls, i):
        return datetime.min if i == 0 else datetime.fromtimestamp(i / 10000 - 62135578800)
