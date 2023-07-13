import pandas as pd
import clickhouse_connect

class Builder:
    def __init__(self, client=None, ):
        self.table = None
        self.filter_column = None
        self.filter_variables = None
        self.holder = None



