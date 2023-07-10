import graphviz as gz
import logging
logging.basicConfig(level=20, filename="custom_painter_log.log",
                            format="%(asctime)s %(levelname)s %(message)s")

class CustomPainter():

    def create(self,
               nodes,
               edges,
               file_name,
               format='svg',
               node_style_metric={'shape': 'plaintext', 'color': '#2d137d', 'fontcolor': '#2d137d',
                                         'fontsize': '12.0', 'size': '2', 'image': '1.png'},
               edge_style_metric={'color': '#2d137d', 'fontcolor': '#2d137d', 'fontsize': '9.0'}):


        ps = gz.Digraph(file_name, node_attr=node_style_metric, edge_attr=edge_style_metric)

        try:
            for g_node in nodes:
                metric = nodes.get(g_node).metrics.get('count')
                if g_node == 'startevent':
                    ps.node(g_node, image='', label='')
                elif g_node == 'endevent':
                    ps.node(g_node, image='', label='')
                else:
                    ps.node(g_node, label=r'' + g_node + '\n' + str(metric) + '', )
        except:
            logging.error(f"nodes metrics error")

        try:
            for g_edge in edges:
                metric = edges.get(g_edge).metrics.get('count')
                if metric == None:
                    ps.edge(g_edge[0], g_edge[1])
                else:
                    ps.edge(g_edge[0], g_edge[1], label=str(metric))
        except:
            logging.error(f"edges metrics error")


        ps.format = format
        #cleanup=True - не сохранять файлы .gv
        ps.render(cleanup=True)
        logging.info(f"call create: file_name - {file_name}, format - {format}")



