import clickhouse_connect

class Builder:
    def __init__(self, table=None, filter_column=None, filter_variables=None, query=None,client=None,table_agg=None,
                 variables=None):
        self.table = 'main_table'
        self.filter_column = None
        self.filter_variables = None
        self.query = None
        self.client = {'host': 'pheerses.space',
                       'port': 8123,
                       'username': 'practice',
                       'password': 'secretKey_lhv323as5vc_d23k32mk'}
        self.table_agg = None
        self.variables = None

    def column(self, table=None, client=None, span='days'):
        if client != None:
            self.client = client
        if table != None:
            self.table = table
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
        #Недоделанная часть расчитаная на календарные периоды
        else:
            self.query = self.client.query('select count(case_id), dt from (select case_id, min(date(start_time)) as dt '
                                       'from ' + self.table + ' group by case_id ORDER BY dt) group by dt')

    def column_query(self,filter_column,table=None, client=None ):
        if client != None:
            self.client = client
        if table != None:
            self.table = table
        self.filter_column = filter_column
        self.table = table
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])
        self.query = self.client.query('select * from ' + self.table + ' where case_id in (select case_id'
                                       ' from main_table_agg where duration/(60*60*24) between '
                                       + filter_column[0] + ' and  ' + filter_column[1] + ')')

    def get_variables(self, filter_column=None, table='main_table', client=None):
        if client != None:
            self.client = client
        if table != None:
            self.table = table
        self.table_agg = table + '_agg'
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])
        if filter_column != None:
            self.filter_column = filter_column
            self.query = self.client.query('select count(case_id) as count_case, variant from' + self.table_agg +
                                           ' where duration/(60*60*24) between' + self.filter_column[0] +
                                           'and' + self.filter_column[1] +
                                           ' group by variant order by count(case_id) desc')

        else:
            self.query = self.client.query('select count(case_id), count(case_id)*100/(select count(case_id)' +
                                           ' from ' + self.table + '_agg), variant from ' + self.table +
                                           ' group by variant order by count(case_id) desc')

    def variables_query(self, filter_column=None,  table=None, client=None):
        if client != None:
            self.client = client
        if table != None:
            self.table = table
        self.table = table
        self.filter_column = filter_column
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])
        if filter_column != None:
            self.query = self.client.query('select * from ' + self.table + 'where case_id in (select case_id from'
                                           ' main_table_agg where variant in ' + self.variables +
                                           ') and duration/(60*60*24) beetween'
                                           + self.filter_column[0] + ' ' + self.filter_column[1])
        else:
            self.query = self.client.query('select * from ' + self.table + 'where case_id in (select case_id from'
                                           'main_table_agg where variant in ' + self.variables + ')')




