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
            self.query = self.client.query('select count(case_id), cast(round(max(diff)) as smallint),'
                                           ' floor(diff/(select max(diff)/10 from '
                                           '(select case_id, date_diff(second, min(start_time),'
                                           ' max(end_time))/(60*60*24) as diff '
                                           'from ' + self.table + ' group by case_id))) as a from (select case_id, '
                                           'date_diff(second, min(start_time), max(end_time))/(60*60*24) as diff '
                                           'from ' + self.table + ' group by case_id order by diff)'
                                           ' group by a order by a')
        else:
            self.query = self.client.query('select count(case_id), dt from (select case_id, min(date(start_time)) as dt '
                                       'from ' + self.table + ' group by case_id ORDER BY dt) group by dt')

    def column_query(self,table,client,filter_column):
        self.filter_column = filter_column
        self.table = table
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])
        self.query = self.client.query('select * from ' + self.table + ' where case_id in (select case_id'
                                       ' from main_table_agg where duration/(60*60*24) between '
                                       + filter_column[0] + ' and  ' + filter_column[1] + ')')

    def get_variables(self,table,client,filter_column = None):
        self.table = table
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])
        if filter_column != None:
            self.filter_column = filter_column

        else:
            self.query = self.client.query('select count(case_id), count(case_id)*100/(select count(distinct case_id)'
                                           ' from ' + self.table + '), a from (select case_id, arrayMap((x)->x[1],'
                                           ' arraySort((x)->toDateTime(x[2]), '
                                           ' groupArray([activity,toString(start_time)]))) as a'
                                           ' from ' + self.table + 'group by case_id)'
                                           ' group by a order by count(case_id) desc')

    def variables_query(self,table,client,filter_variables,filter_column):
        self.table = table
        self.filter_variables = filter_variables
        self.filter_column = filter_column
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])
        self.query = self.client.query('select * from ' + self.table + 'where case_id in (select case_id from'
                                       ' main_table_agg where variant in )')


