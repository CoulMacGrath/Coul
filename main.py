#Обработка данных и подключение к БД
from sberpm import DataHolder
from sberpm.metrics import ActivityMetric

import pandas as pd
import clickhouse_connect

import custom_painter
import query_builder
import column_diagram
import processing_variants
import metric



#Различные варинаты майнеров
from sberpm.miners import HeuMiner, SimpleMiner,CausalMiner


class PythonMain:
    class Connect:
        def __init__(self, client=None, table=None, query1=None, query2=None, miner=None,
                     data_purity=None,filter_column=None,filter_variables=None,
                     logfile=None, top_combine_variables=None, other_сleared_variables=None,
                     column_variabls=None,holder=None,diagram=None, image_graph=None,
                     count_metric=None, graph=None, connect=None,image_column=None,
                     time_metric=None,all_variables=None,column_query=None):
            self.client = {'host': 'pheerses.space',
                           'port': 8123,
                           'username': 'practice',
                           'password': 'secretKey_lhv323as5vc_d23k32mk'}
            self.connect = None
            self.table = 'main_table'
            self.query1 = None
            self.query2 = None
            self.miner = None
            self.data_purity = None
            self.filter_column = None
            self.filter_variables = None
            self.logfile = None
            self.top_combine_variables = None
            self.other_сleared_variables = None
            self.all_variables = None
            self.column_variables = None
            self.holder = None
            self.diagram = None
            self.image_graph = None
            self.image_column = None
            self.count_metric = None
            self.graph = None
            self.time_metric = None
            self.column_query = None
        def get_main_holder(self,table=None, miner=None, client=None):
            if client != None:
                self.connect = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                        username=client['username'], password=client['password'])
            if table != None:
                self.table = table
            lst_data = []
            list_id = self.connect.query('SELECT case_id, activity, start_time, end_time FROM ' + self.table)
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

            self.holder.check_or_calc_duration()
            self.holder.data.head()
            self.holder.get_grouped_data(self.holder.activity_column, self.holder.start_timestamp_column).head()

            activity_metric = ActivityMetric(self.holder, time_unit='d')
            activity_metric.calculate_time_metrics()
            activity_metric.apply().head()
            self.count_metric = activity_metric.count().to_dict()

            if self.miner != None:
                self.get_miner()
            else:
                self.get_miner(miner)


        def get_filtred_holder(self, filter_column, filter_variables, client=None, table=None):
            return


        def first_start(self,table = None, miner=None, client=None):
            if client != None:
                self.client = client
            if table != None:
                self.table = table
        #Обрабатывает данные и создает граф
            self.get_main_holder(table=self.table,miner=self.miner,client=self.client)
            self.miner.apply()
            self.graph = self.miner.graph
            self.graph.add_node_metric('count', self.count_metric)
        #Сбор вариантов для дальнейшей обработки
            self.query1 = query_builder.Builder()
            self.query1.get_variables(table=self.table, client=self.client)
            self.query1 = self.query1.query

            self.column_variables = processing_variants.Connect()
            self.column_variables.apply(self.query1)
            self.column_variables.combine_variables()
        #Собирает метрики по всем вариантам
            self.all_variables = self.column_variables.all_variables
            self.time_metric = metric.Metric().culc_edge_metric(self.all_variables,self.client)
            self.time_metric = metric.Metric().join_metrics(self.time_metric)
            self.column_variables = self.column_variables.column_variables

        #Сохраняет в папку диаграмму в формате "названиетаблицы_column"
            self.query2 = query_builder.Builder()
            self.query2.get_column(table=self.table, client=self.client)
            self.query2 = self.query2.query

            self.column_query = column_diagram.Calc_diagrams()
        #column_query содержит данные для фильтрации по колонкам
            self.column_query = self.column_query.calc_diagram_days(self.query2)

            self.image_column = column_diagram.DiagramPainter()
            self.image_column.create_diagram_days(self.column_query, '#e06666',self.table)

        #Сохраняет свг граф и возвращает картинку в формате base64
            self.image_graph = custom_painter.CustomPainter().create(nodes=self.graph.nodes, edges=self.graph.edges,
                                                                     file_name='main_all', format='svg',
                                                                     metric=self.time_metric)

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
    test.first_start()








