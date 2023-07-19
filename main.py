#Обработка данных и подключение к БД
from sberpm import DataHolder

import pandas as pd
import clickhouse_connect
import query_builder
import column_diagram


#Различные варинаты майнеров
from sberpm.miners import HeuMiner, SimpleMiner,CausalMiner


class PythonMain:
    class Connect:
        def __init__(self, client=None, table=None, query1=None, query2=None, miner=None,
                     data_purity=None,filter_column=None,filter_variables=None,
                     logfile=None, top_combine_variables=None, other_сleared_variables=None,
                     holder=None,diagram=None):
            self.client = None
            self.table = None
            self.query1 = None
            self.query2 = None
            self.miner = None
            self.data_purity = None
            self.filter_column = None
            self.filter_variables = None
            self.logfile = None
            self.top_combine_variables = None
            self.other_сleared_variables = None
            self.holder = None
            self.diagram = None
        def get_main_holder(self,table = 'main_table', miner=None, client={'host': 'pheerses.space',
                                                                           'port': 8123,
                                                                           'username': 'practice',
                                                                           'password': 'secretKey_lhv323as5vc_d23k32mk'}):
            if client != None:
                self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                        username=client['username'], password=client['password'])
            if table != None:
                self.table = table
            lst_data = []
            list_id = self.client.query('SELECT case_id, activity, start_time, end_time FROM ' + self.table)
            for i in list_id.result_rows:
                lst_data.append(i)
                lst_name = list_id.column_names
            df = pd.DataFrame(lst_data, columns=lst_name)

            self.holder = DataHolder(data=df,
                                     id_column='case_id',
                                     activity_column='activity',
                                     start_timestamp_column='start_time',
                                     end_timestamp_column='end_time',
                                     time_format='%Y-%m-%d %I:%M:%S')
            if self.miner != None:
                self.get_miner()
            else:
                self.get_miner(miner)
        def get_filtred_holder(self, filter_column, filter_variables, client=None, table=None):
            if


        def first_start(self,table = 'main_table', miner=None, client={'host': 'pheerses.space',
                                                                       'port': 8123,
                                                                       'username': 'practice',
                                                                       'password': 'secretKey_lhv323as5vc_d23k32mk'}):
            self.holder = self.get_main_holder(table=,miner=)


        def get_column_data(self,client=None,table=None):
            if client != None:
                self.client = client
            if table != None:
                self.table = table
            self.query = query_builder.Builder.column(client=self.client,table=self.table,span='days')
            self.diagram = column_diagram.Calc_diagrams.calc_diagram_days(self.query)




        def apply(self,client=None,table=None):
            if client != None:
                self.client = client
            if table != None:
                self.table = table
            if self.filter_column == None and self.filter_variables == None:
                self.query = query_builder.first_query(self.table, self.client)
            elif self.filter_column != None and self.filter_variables == None:
                self.query = query_builder.column_query(self.filter_column, self.table, self.client,)
            elif self.filter_variables != None and self.filter_column == None :
                self.query = query_builder.variables_query(self.filter_variables, self.table, self.client)
            else:
                self.query = query_builder.full_query(self.filter_column, self.filter_variables, self.table, self.client)

        def get_miner(self,miner=None,data_purity=0.8):
            self.data_purity = data_purity
            if miner == 'CasualMiner':
                self.miner = CausalMiner(self.holder)
            elif miner == 'SimpleMiner':
                self.miner = SimpleMiner(self.holder)
            else:
                self.miner = HeuMiner(self.holder, threshold=self.data_purity)







if __name__ == '__main__':
    test = PythonMain.Connect()
    test.get_main_holder()








