import clickhouse_connect
import logging
from itertools import permutations
import networkx as nx
from networkx.algorithms import isomorphism

class Connect:
    def __init__(self, query=None, top_variables=None, other_variables=None,
                 logfile=None, top_combine_variables=None, other_сleared_variables=None,
                 column_variables=None):
        self.query = None
        self.top_variables = None
        self.other_variables = None
        self.logfile = None
        self.top_combine_variables = None
        self.other_сleared_variables = None
        self.column_variables = None

    def apply(self,query):
        self.query = query
        self.top_variables = []
        self.other_variables = []
        result = self.query.result_rows

        for item in result:
            if item[1] > 1:
                self.top_variables.append(item)
            else:
                self.other_variables.append(item)
        logging.info('get a query')

    def combine_variables_isomorph(self):
        result_top = self.top_variables
        self.top_combine_variables = []
        self.other_сleared_variables = self.other_variables
        for (index,res) in enumerate(result_top):
            a = (str(index), res)
            print(a)
            g1 = nx.Graph()
            for (intex, i) in enumerate(res[2]):
                try:
                    g1.add_node(i)
                    g1.add_edge(res[intex], res[intex + 1])
                except:
                    k = 0
            for res2 in self.other_variables:
                g2 = nx.Graph()
                for (inter, i) in enumerate(res2[2]):
                    try:
                        g2.add_node(i)
                        g2.add_edge(res2[inter], res2[inter + 1])
                    except:
                        k=0
                GM = isomorphism.GraphMatcher(g1, g2)
                if GM.is_isomorphic():
                    b = (str(index), res2)
                    print(b)
                    self.top_combine_variables.append(b)
                    self.other_сleared_variables.remove(res2)
        logging.info('combine variables')
        return(self.top_combine_variables)

    def combine_variables(self):
        result_top = self.top_variables
        self.top_combine_variables = []
        self.column_variables = []
        self.other_сleared_variables = self.other_variables
        for (i,elem) in enumerate(result_top):
            c = 0
            d = 0
            a = (str(i), elem)
            c = a[1][0]
            d = a[1][1]
            self.top_combine_variables.append(a)
            list1 = elem[2]
            for (e,elem2) in enumerate(self.other_variables):
                list2 = elem2[2]
                if set(list1) != set(list2):
                    continue

                # Получение всех перестановок списка list1
                perm_list1 = list(permutations(list1, len(list1)))

                # Проверка схожести списков с учетом перестановок
                for perm in perm_list1:
                    seq_list1 = list(perm)
                    seq_list2 = list2[:len(seq_list1)]

                    if seq_list1 == seq_list2:
                        b = (str(i), elem2)
                        c += elem2[0]
                        d += elem2[1]
                        self.top_combine_variables.append(b)
                        self.other_сleared_variables.remove(elem2)
                continue
            top_cort = (str(i),c,d)
            self.column_variables.append(top_cort)
        oc = 0
        od = 0
        for element in self.other_сleared_variables:
            oc += element[0]
            od += element[1]
        other_cort = ('other',oc,od)
        self.column_variables.append(other_cort)

        return (self.top_combine_variables)









if __name__ == '__main__':
    #Поключение к clickhouse
    client = clickhouse_connect.get_client(host='pheerses.space', port=8123, username='practice',
                                           password='secretKey_lhv323as5vc_d23k32mk')
    #Получение данных
    graphs = client.query('select count(case_id), count(case_id)*100/(select count(distinct case_id) from main_table),'
                          ' a from (select case_id, arrayMap((x)->x[1], arraySort((x)->toDateTime(x[2]), '
                          'groupArray([activity,toString(start_time)]))) as a  from main_table group by case_id)'
                          ' group by a order by count(case_id) desc')
    connect = Connect()
    connect.apply(graphs)
    #connect.combine_variables_isomorph()
    connect.combine_variables()
    #print(connect.top_variables)
    print(connect.column_variables)
