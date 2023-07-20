import clickhouse_connect
class Metric:

    def __init__(self, combine_variables=None, client=None, data=None, edge_metric=None, new_metric_edge_dict=None ):
        self.combine_variables = None
        self.client = None
        self.data = None
        self.edge_metric = None
        self.new_metric_edge_dict = None

    def culc_edge_metric(self, combine_variables, client):

        self.edge_metric = []
        self.client = clickhouse_connect.get_client(host=client['host'], port=client['port'],
                                                    username=client['username'], password=client['password'])
        self.combine_variables = combine_variables

        for i in range(len(self.combine_variables)):

            metric = self.client.query(f'select avgForEach(b) from (select groupArray(activity) as a, '
                                  f'arrayMap((x, y)-> date_diff(second, toDateTime(x), toDateTime(y))/(60*60*24), '
                                  f'groupArray(start_time), arrayPushBack(arrayPopFront(groupArray(end_time) as ld), ld[-1])) as b '
                                  f'from (select * from main_table order by start_time, end_time, activity) group by case_id) '
                                  f'having a = {self.combine_variables[i][1][2]}')
            metric_edge_dict = {}
            for j in range(len(self.combine_variables[i][1][2])-1):
                try:
                    first_edge = self.combine_variables[i][1][2][j]
                    second_edge = self.combine_variables[i][1][2][j + 1]
                    metric_edge = metric.result_rows[0][0][j]
                    metric_edge_dict[first_edge, second_edge] = metric_edge
                except:
                    continue
            if len(metric_edge_dict)>0:
                self.edge_metric.append([metric_edge_dict, self.combine_variables[i][1][0]])

        return self.edge_metric

    def join_metrics(self, data, start, end):

        self.data = data
        self.new_metric_edge_dict = {}
        new_sum_e_dict = {}
        new_total_count_dict = {}
        #нормально переделать, добавить начальную точку
        for i in range(start, end):
            for k, v in self.data[i][0].items():
                if k in self.new_metric_edge_dict:
                    #умножить каждый переход на индивидуальное количество -> все сложить -> разделить на общее количество
                    self.new_metric_edge_dict[k] = v
                    new_sum_e_dict[k] = new_sum_e_dict[k] + v * self.data[i][1]
                    new_total_count_dict[k] = new_total_count_dict[k] + self.data[i][1]
                else:
                    self.new_metric_edge_dict[k] = v
                    new_sum_e_dict[k] = v * self.data[i][1]
                    new_total_count_dict[k] = self.data[i][1]

            for i in self.new_metric_edge_dict:
                self.new_metric_edge_dict[i] = new_sum_e_dict[i]/new_total_count_dict[i]

        return self.new_metric_edge_dict
