

class Metric:
    def culc_edge_metric(self, combine_variables, client):

        edge_metric = []
        for i in range(len(combine_variables)-1):

            metric = client.query(f'select avgForEach(b) from (select groupArray(activity) as a, '
                                  f'arrayMap((x, y)-> date_diff(second, toDateTime(x), toDateTime(y))/(60*60*24), '
                                  f'groupArray(start_time), arrayPushBack(arrayPopFront(groupArray(end_time) as ld), ld[-1])) as b '
                                  f'from (select * from main_table order by start_time, end_time, activity) group by case_id) '
                                  f'having a = {combine_variables[i][1][2]}')
            metric_edge_dict = {}
            for j in range(len(combine_variables[i][1][2])-1):
                try:
                    first_edge = combine_variables[i][1][2][j]
                    second_edge = combine_variables[i][1][2][j + 1]
                    metric_edge = metric.result_rows[0][0][j]
                    metric_edge_dict[first_edge, second_edge] = metric_edge
                except:
                    continue
            if len(metric_edge_dict)>0:
                edge_metric.append(metric_edge_dict)

        return edge_metric

    def join_metrics(self, data):

        return 0





