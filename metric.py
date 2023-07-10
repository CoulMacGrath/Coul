

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
                edge_metric.append([metric_edge_dict, combine_variables[i][1][0]])

        return edge_metric

    def join_metrics(self, data):

        new_metric_edge_dict = {}
        new_sum_e_dict = {}
        new_total_count_dict = {}
        #нормально переделать, добавить начальную точку
        for i in range(1,4):#len(data)):
            #print(data[i])

            for k, v in data[i][0].items():
                #print(k, v)
                #print()
                if k in new_metric_edge_dict:
                    #умножить каждый переход на индивидуальное количество -> все сложить -> разделить на общее количество
                    new_metric_edge_dict[k] = v
                    new_sum_e_dict[k] = new_sum_e_dict[k] + v * data[i][1]
                    new_total_count_dict[k] = new_total_count_dict[k] + data[i][1]
                else:
                    new_metric_edge_dict[k] = v
                    new_sum_e_dict[k] = v * data[i][1]
                    new_total_count_dict[k] = data[i][1]

            for i in new_metric_edge_dict:
                new_metric_edge_dict[i] = new_sum_e_dict[i]/new_total_count_dict[i]

        for k, v in new_metric_edge_dict.items():
            print(k, v)

        return 0







