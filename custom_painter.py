import base64

import graphviz as gz
import logging
logging.basicConfig(level=20, filename="custom_painter_log.log",
                            format="%(asctime)s %(levelname)s %(message)s")



class CustomPainter():

    def __init__(self, nodes=None, edges=None, file_name=None, time_metric=None, format=None, new_color=None, node_style_metric=None, edges_style_metric=None):
        self.nodes = None
        self.edges = None
        self.file_name = None
        self.time_metric = None
        self.new_color = new_color
        #Базовые настройки
        self.format = 'svg'
        self.node_style_metric = {'shape': 'plaintext', 'color': self.new_color, 'fontcolor': self.new_color,
                                  'fontsize': '12.0', 'size': '2', 'image' : '1.png', 'imagepos' : 'ml',
                                  'margin' : '0.55,0.0'

                                  }
        self.edge_style_metric = {'color': self.new_color, 'fontcolor': self.new_color, 'fontsize': '12.0'}




    def create(self, nodes, edges, file_name, format, time_metric):

        self.nodes = nodes
        self.edges = edges
        self.file_name = file_name
        self.format = format
        self.time_metric = time_metric


        ps = gz.Digraph(self.file_name, node_attr=self.node_style_metric, edge_attr=self.edge_style_metric)

        try:
            for g_node in nodes:
                metric = nodes.get(g_node).metrics.get('count')
                if g_node == 'startevent':
                    ps.node(g_node, image='start.png', label='', )
                elif g_node == 'endevent':
                    ps.node(g_node, image='end.png', label='')
                else:
                    ps.node(g_node, label=g_node + '\n' + str(metric) + '\l')

        except:
            logging.error(f"nodes metrics error")

        try:
            for g_edge in edges:
                if (g_edge in self.time_metric):
                    metric = round(self.time_metric[g_edge], 2)
                else:
                    metric = None
                if metric == None:
                    ps.edge(g_edge[0], g_edge[1])
                else:
                    ps.edge(g_edge[0], g_edge[1], label=str(metric) + "d")
        except:
            logging.error(f"edges metrics error")



        ps.format = self.format
        # cleanup=True - не сохранять файлы .gv
        ps.render(cleanup=True)
        logging.info(f"call create: file_name - {self.file_name}, format - {self.format}")
        with open(str(self.file_name) + ".gv." + self.format, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read())
            image_file.close()
            self.base64image = encoded_string

        logging.info(f"call create: file_name - {self.file_name}, format - {self.format}")



