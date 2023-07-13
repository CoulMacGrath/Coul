#Обработка данных и подключение к БД
from sberpm import DataHolder
from sberpm.metrics import ActivityMetric, TransitionMetric, IdMetric, TraceMetric, UserMetric

import pandas as pd
import clickhouse_connect
import query_builder

#Graphviz
from sberpm.visual import GraphvizPainter,ChartPainter
import graphviz as gz

#Различные варинаты майнеров
from sberpm.miners import HeuMiner, SimpleMiner,CausalMiner,AlphaMiner,AlphaPlusMiner, InductiveMiner, CorrelationMiner


class PythonMain:
    class Connect:
        def __init__(self, client=None, table=None, query1=None, query2=None, miner=None,
                     data_purity=None,filter_column=None,filter_variables=None,
                     logfile=None, top_combine_variables=None, other_сleared_variables=None,
                     holder=None):
            self.client = None
            self.table = None
            self.query1 = None
            self.query2 = None
            self.miner = None
            self.data_purity = 0.8
            self.filter_column = None
            self.filter_variables = None
            self.logfile = None
            self.top_combine_variables = None
            self.other_сleared_variables = None
            self.holder = None

        def get_main_holder(self,table,client,miner=None):
            self.client = clickhouse_connect.get_client(host='pheerses.space', port=8123, username='practice',
                                                   password='secretKey_lhv323as5vc_d23k32mk')
            self.table = table

            # Получение данных и преобразование в DataFrame
            # Соединение часто недостаточно быстрое чтобы заниматсья дебагом
            lst_data = []
            list_id = client.query('SELECT case_id, activity, start_time, end_time FROM {self.table}')
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
                self.miner = self.get_miner()
            else:
                self.miner = self.get_miner(miner)




        def apply(self,client):
            self.client = clickhouse_connect.get_client(host=client.host, port=8123, username=client.username, password= client.password)
            if self.filter_column == None and self.filter_variables == None:
                self.query = query_builder.first_query(self.filter_column,self.table)
            elif self.filter_column != None and self.filter_variables == None:
                self.query = query_builder.column_query(self.filter_column,self.table)
            elif self.filter_variables != None and self.filter_column == None :
                self.query = query_builder.variables_query(self.filter_variables, self.table)
            else:
                self.query = query_builder.full_query(self.filter_column,self.filter_variables,self.table)

        def get_miner(self,miner=None):
            if miner == 'CasualMiner':
                self.miner = CausalMiner(self.holder)
            elif miner == 'SimpleMiner':
                self.miner = SimpleMiner(self.holder)
            else:
                self.miner = HeuMiner(self.holder, threshold=self.data_purity)












