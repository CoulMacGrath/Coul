import clickhouse_connect

class Builder:
    def __init__(self, table = None, filter_column = None, filter_variables = None, query = None,client = None):
        self.table = None
        self.filter_column = None
        self.filter_variables = None
        self.query = None
        self.client = None

    def column(self,table,span,client):
        self.table = table
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                        username=client['username'], password=client['password'])
        if span == 'days':
            self.query = self.client.query('select count(case_id), cast(round(max(diff)) as smallint), floor(diff/(select max(diff)/10 from '
                        '(select case_id, date_diff(second, min(start_time), max(end_time))/(60*60*24) as diff '
                        'from ' + self.table + ' group by case_id))) as a from (select case_id, '
                        'date_diff(second, min(start_time), max(end_time))/(60*60*24) as diff '
                        'from {table} group by case_id order by diff) group by a order by a')
        else:
            self.query = self.client.query('select count(case_id), dt from (select case_id, min(date(start_time)) as dt '
                                       'from ' + self.talbe + ' group by case_id ORDER BY dt) group by dt')

    def filter_column(self,client,table):
        self.table = table
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])

    def filter_variables(self):
        self.table = table
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])


