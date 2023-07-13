import graphviz as gz
import logging
import base64


logging.basicConfig(level=20, filename="custom_painter_log.log",
                            format="%(asctime)s %(levelname)s %(message)s")


class CustomPainter():

    def __init__(self, nodes=None, edges=None,  logfile=None,
                 node_style_metric=None, edges_style_metric=None,
                 theme=None, format=None, base64image=None):
        self.nodes = None
        self.edges = None
        self.file_name = None
        self.format = 'svg'
        self.node_style_metric = {'shape': 'plaintext', 'color': '#2d137d', 'fontcolor': '#2d137d',
                                         'fontsize': '12.0', 'size': '2', 'image': '1.png'}
        self.edges_style_metric= {'color': '#2d137d', 'fontcolor': '#2d137d', 'fontsize': '9.0'}
        self.theme = None
        self.logfile = None
        self.base64image = None




    def create(self, nodes, edges, file_name):
        self.nodes = nodes
        self.edges = edges
        self.file_name = file_name

        #logging.basicConfig(level=self.logfile.level, filename=self.logfile.filename,
        #                    format=self.logfile.format)

        ps = gz.Digraph(self.file_name, node_attr=self.node_style_metric, edge_attr=self.edges_style_metric)

        try:
            for g_node in self.nodes:
                metric = self.nodes.get(g_node).metrics.get('count')
                if g_node == 'startevent':
                    ps.node(g_node, image='', label='')
                elif g_node == 'endevent':
                    ps.node(g_node, image='', label='')
                else:
                    ps.node(g_node, label=r'' + g_node + '\n' + str(metric) + '', )
        except:
            logging.error(f"nodes metrics error")

        try:
            for g_edge in self.edges:
                metric = self.edges.get(g_edge).metrics.get('count')
                if metric == None:
                    ps.edge(g_edge[0], g_edge[1])
                else:
                    ps.edge(g_edge[0], g_edge[1], label=str(metric))
        except:
            logging.error(f"edges metrics error")


        ps.format = self.format
        #cleanup=True - не сохранять файлы .gv
        ps.render(cleanup=True)
        with open(str(self.file_name)+".gv."+self.format, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read())
            image_file.close()
            self.base64image = encoded_string


        logging.info(f"call create: file_name - {self.file_name}, format - {self.format}")






