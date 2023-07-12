#Обработка данных и подключение к БД
from sberpm import DataHolder
from sberpm.metrics import ActivityMetric, TransitionMetric, IdMetric, TraceMetric, UserMetric

import pandas as pd
import clickhouse_connect

#Graphviz
from sberpm.visual import GraphvizPainter,ChartPainter
import graphviz as gz

#Различные варинаты майнеров
from sberpm.miners import HeuMiner, SimpleMiner,CausalMiner,AlphaMiner,AlphaPlusMiner, InductiveMiner, CorrelationMiner


class PythonMain:
    class Connect:
        def __init__(self, client=None, graphs=None, top_variables=None, other_variables=None,
                     logfile=None, top_combine_variables=None, other_сleared_variables=None):
            self.client = None
            self.query = None
            self.miner = None
            self. = None
            self.logfile = None
            self.top_combine_variables = None
            self.other_сleared_variables = None

