#Обработка данных и подключение к БД
from sberpm import DataHolder
from sberpm.metrics import ActivityMetric, TransitionMetric, IdMetric, TraceMetric, UserMetric

import pandas as pd
import clickhouse_connect

#Graphviz
from sberpm.visual import GraphvizPainter,ChartPainter
import graphviz as gz
import custom_painter as c_p

#Различные варинаты майнеров
from sberpm.miners import HeuMiner, SimpleMiner,CausalMiner,AlphaMiner,AlphaPlusMiner, InductiveMiner, CorrelationMiner

def  initializating_pm():

#Получение и обработка БД
    #Обработка из локального файла, лучше использовать если сидишь в отладчике, иначе будет собирать DataHolder от 10с до 40с, локально в разы быстрее
    #data_holder = DataHolder(data='test2.csv', id_column='case_id',
    #                         activity_column='activity',
    #                         start_timestamp_column='start time',
    #                         end_timestamp_column='end time',
    #                         time_format='%m/%d/%Y %I:%M:%S %p')

    client = clickhouse_connect.get_client(host='pheerses.space', port=8123, username='practice', password='secretKey_lhv323as5vc_d23k32mk')

    # Получение данных и преобразование в DataFrame
    #Соединение часто недостаточно быстрое чтобы заниматсья дебагом
    lst_data = []
    list_id = client.query('SELECT case_id, activity, start_time, end_time FROM main_table')
    for i in list_id.result_rows:
        lst_data.append(i)
        lst_name = list_id.column_names
    df = pd.DataFrame(lst_data, columns=lst_name)

    data_holder = DataHolder(data=df,
                             id_column='case_id',
                             activity_column='activity',
                             start_timestamp_column='start_time',
                             end_timestamp_column='end_time',
                             time_format='%Y-%m-%d %I:%M:%S')

    data_holder.check_or_calc_duration()
    data_holder.data.head()
    data_holder.get_grouped_data(data_holder.activity_column, data_holder.start_timestamp_column).head()

    activity_metric = ActivityMetric(data_holder,time_unit='d')
    activity_metric.calculate_time_metrics()
    activity_metric.apply().head()
    count_metric = activity_metric.count().to_dict()
    time_metric = activity_metric.mean_duration().to_dict()
    #transition_metric = TransitionMetric(data_holder,time_unit='d')
    #transition_metric.apply().head()
    #edges_count_metric = activity_metric.count().to_dict()

    #trace_metric = TraceMetric(data_holder,time_unit='d')
    #trace_metric.apply().head()
    #edges_count_metric = trace_metric.mean_duration().to_dict()

    transition_metric = TransitionMetric(data_holder,time_unit='d')
    transition_metric.apply().head()
    edges_count_metric = transition_metric.mean_duration().to_dict()

#Область объявления майнеров

    #Обявление списка на отрисовку
    miner_graphs = []

    #Hei miner
    heu_miner = HeuMiner(data_holder, threshold=0.8)
    heu_miner.apply()
    miner_graphs.append(heu_miner.graph)

    #Simple Miner
    #simple_miner = SimpleMiner(data_holder)
    #simple_miner.apply()
    #miner_graphs.append(simple_miner.graph)

    #Casual Miner
    #casual_miner = CausalMiner(data_holder)
    #casual_miner.apply()
    #miner_graphs.append(casual_miner.graph)

    #Alpha Miner
    #alpha_miner = AlphaMiner(data_holder)
    #alpha_miner.apply()
    #miner_graphs.append(alpha_miner.graph)

    #AlphaPlus Miner
    #alphaplus_miner = AlphaPlusMiner(data_holder)
    #alphaplus_miner.apply()
    #miner_graphs.append(alphaplus_miner.graph)

    #InductiveMiner Miner
    #inductive_miner = InductiveMiner(data_holder)
    #inductive_miner.apply()
    #miner_graphs.append(inductive_miner.graph)

    #CorrelationMiner Miner
    #correlation_miner = CorrelationMiner(data_holder)
    #correlation_miner.apply()
    #correlation_miner.append(correlation_miner.graph)

#Графики
    #painter = ChartPainter(data_holder)
    #painter.hist_activity_of_dur(top= False, use_median=False)
#Модуль отисовки
    for (index,elem) in enumerate(miner_graphs):
        try:
            elem.add_node_metric('count',count_metric)
            elem.add_edge_metric('count', edges_count_metric)
        except:
            print('ошибочка')
        painter = GraphvizPainter()
        #painter.apply(elem,node_style_metric='count', edge_style_metric='count')
        custom_graph(elem.nodes,elem.edges,'graph' + str(index),format='svg')
        #painter.write_graph('graph'+str(index)+'.svg',format='svg')


#Кастомная функция для отрисовки готовых графов, создана по причине отсутствия стилизации средств встроенных в библиотеку sberPm
def custom_graph(nodes,edges,file,format='svg'):
    img = c_p.CustomPainter()
    img.create(nodes,edges,file)
    #ps = gz.Digraph(file, node_attr={'shape': 'plaintext', 'color': '#2d137d', 'fontcolor': '#2d137d',
    #                                 'fontsize': '12.0', 'size': '2', 'image': '1.png'},
    #                                  edge_attr={'color': '#2d137d', 'fontcolor': '#2d137d'})

 #Перенос вершин графов и их весов из майнера в Digraph
    #Перенос вершин графов
    #for g_node in nodes:
    #    metric = nodes.get(g_node).metrics.get('count')
   #     if g_node == 'startevent':
    #        ps.node(g_node, image='', label='')
   #     elif g_node == 'endevent':
    #        ps.node(g_node, image='', label='')
     #   else:
      #      ps.node(g_node,label=r'' + g_node + '\n'+'             ' + str(metric) + '\l')

    #Перенос весов графов
 #   for g_edge in edges:
  #      metric = edges.get(g_edge).metrics.get('count')
   #     if metric == None:
    #        ps.edge(g_edge[0],g_edge[1])
     #   else:
      #      ps.edge(g_edge[0],g_edge[1],label=str(metric))


    #Вывод svg файла
  #  ps.format = format
   # ps.render()

if __name__ == '__main__':
    initializating_pm()

