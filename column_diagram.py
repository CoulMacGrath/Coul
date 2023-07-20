import clickhouse_connect
import logging
import matplotlib.pyplot as plt

logging.basicConfig(level=20, filename="column_diagram_log.log",
                            format="%(asctime)s %(levelname)s %(message)s")

class Calc_diagrams():

    def __init__(self, data=None, list_days=None, list_month=None, column_image=None, table=None):
        self.data = None
        self.list_days = None
        self.list_month = None
        self.column_image = None


    def calc_diagram_days(self, data):
        # Получение данных
        self.data = data

        self.list_days = []  # [к-во уникальных Id, №дня, №дня]
        start_day = 0
        for i in range(0, len(self.data.result_rows)-1):
            self.list_days.append([self.data.result_rows[i][0], start_day, self.data.result_rows[i][1]])
            start_day = self.data.result_rows[i][1]

        logging.info(f"call calc_diagram_days: {self.list_days}")
        # для получения часов/минут перевести my_start, step_end
        return self.list_days

    def calc_diagram_months(self, data):

        # Получение данных
        self.data = data

        self.list_month = [[0, 1], [0, 2], [0, 3], [0, 4], [0, 5], [0, 6], [0, 7], [0, 8], [0, 9], [0, 10], [0, 11],
                      [0, 12]]  # [к-во уникальных Id, №месяца]

        # Добавление данных [к-во уникальных Id, № месяца] в список
        # Считаем количество уникальных Id для конкретного месяца
        for i in self.data.result_rows:
            self.list_month[i[1].month - 1][0] = self.list_month[i[1].month - 1][0] + i[0]

        logging.info(f"call calc_diagram_months: {self.list_month}")
        return self.list_month

class DiagramPainter():

    def create_diagram(self, days, cases, color, xlabel,table):
        fig, ax = plt.subplots()

        ax.bar(days, cases, color=color, width=0.64, )

        ax.set_xlabel(xlabel, color='#999999')
        plt.yticks(color='#3c3939')
        plt.xticks(rotation=315, color='#3c3939')
        plt.grid(axis='y', linewidth=0.2)
        plt.locator_params(axis='y', nbins=14)
        plt.savefig(str(table) + '_column')
        return plt


    def create_diagram_days(self, data, color,table):
        # [[7179, 0, 17], [10006, 17, 35], [10331, 35, 52], [12080, 52, 70], [10303, 70, 87], [5976, 87, 105], [5432, 105, 122], [2261, 122, 140], [195, 140, 157], [27, 157, 175]]
        days = []
        cases = []
        for d in data:
            cases.append(d[0])
            days.append(f'{d[1]}-{d[2]}')

        self.create_diagram(days, cases, color, 'Day(s)',table)

    def create_diagram_months(self, data, color):
        months = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August',
             9: 'September', 10: 'October', 11: 'November', 12: 'December'}

        # [[16695, 1], [15689, 2], [15544, 3], [9917, 4], [4706, 5], [1246, 6], [0, 7], [0, 8], [0, 9], [0, 10], [0, 11], [0, 12]]
        days = []
        cases = []
        for d in data:
            cases.append(d[0])
            days.append(f'{months[d[1]]}')

        self.create_diagram(days, cases, color, 'Month(s)')



if __name__ == '__main__':

    client = clickhouse_connect.get_client(host='pheerses.space',
                                           port=8123,
                                           username='practice',
                                           password='secretKey_lhv323as5vc_d23k32mk')

    data_months = client.query('select count(case_id), dt from (select case_id, min(date(start_time)) as dt '
                                         'from main_table group by case_id ORDER BY dt) group by dt')

    data_days = client.query('select count(case_id), cast(round(max(diff)) as smallint), floor(diff/(select max(diff)/10 from '
                             '(select case_id, date_diff(second, min(start_time), max(end_time))/(60*60*24) as diff '
                             'from main_table group by case_id))) as a from (select case_id, '
                             'date_diff(second, min(start_time), max(end_time))/(60*60*24) as diff '
                             'from main_table group by case_id order by diff) group by a order by a')

    diagrams = Calc_diagrams()
    #print(diagrams.calc_diagram_days(data_days))
    #print()
    #print(diagrams.calc_diagram_months(data_months))
    #print()
    painter = DiagramPainter()
    painter.create_diagram_days(diagrams.calc_diagram_days(data_days), '#e06666')
    painter.create_diagram_months(diagrams.calc_diagram_months(data_months), '#e06666')

