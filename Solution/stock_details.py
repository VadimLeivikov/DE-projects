import logs_writer

## Creating a Class for retrieving company's stock details:

class Stock_details:
    def __init__(self, symbol):
        from  sqlalchemy import create_engine, MetaData, Table
        self.symbol = symbol
        self.__engine = create_engine(logs_writer.engine_conf, isolation_level="AUTOCOMMIT")
        self.__conn = self.__engine.connect()
        self.__metadata = MetaData() 
        self.__stock_rates = Table('stock_rates', self.__metadata, autoload_with = self.__engine)
    @property
    @logs_writer.log_property_call
    def name(self):
        from  sqlalchemy import select
        query = select(self.__stock_rates.c.Company_name).where(self.__stock_rates.c.Symbol == self.symbol)
        result = self.__conn.execute(query)
        self.__company_name = result.fetchone()
        return self.__company_name[0]
    
    @logs_writer.log_get_details
    def get_details(self, start_date:str, end_date:str):
        import json
        import pandas as pd
        from datetime import date, timedelta
        from  sqlalchemy import text

        try:
            if (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days > 30: raise ValueError
        except ValueError as e:
            print(f'Date interval should be less than 30 days {e}')
            return None

        query  = '''
                    SELECT Symbol, Company_name, Date, Open, High, Low, Close, "Adj Close", Volume 
                    FROM stock_rates
                    WHERE (Symbol = %(dynamic_symbol)s OR Company_name = %(dynamic_company_name)s)
                            AND (Date BETWEEN %(dynamic_start)s AND %(dynamic_end)s)
                    '''
        df_details = pd.read_sql(query, con = self.__engine, params={"dynamic_symbol": self.symbol\
                                                                     , 'dynamic_company_name':self.__company_name\
                                                                        ,'dynamic_start':date.fromisoformat(start_date)\
                                                                            ,'dynamic_end':date.fromisoformat(end_date)})
        result = df_details.to_json(orient="records")
        parsed = json.loads(result)
        return json.dumps(parsed, indent=4)


## Self-checking:
if __name__ == "__main__":   
    # stock_AA = Stock_details('AA')
    # stock_AA.name
    # print(stock_AA.symbol)
    # print(stock_AA.name)
    # print(stock_AA.get_details('1962-01-02', '1962-01-05'))


    # print()
    # stock_A = Stock_details('A')
    # stock_A.name
    # print(stock_A.symbol)
    # print(stock_AA.name)
    # print(stock_A.get_details('1999-11-18', '1999-12-25'))
    # print(stock_A.get_details('1999-11-18', '1999-11-25'))

    # stock_AAON = Stock_details('AAON')
    # stock_AAON.name
    # print(stock_AAON.symbol)
    # print(stock_AAON.name)
    # print(stock_AAON.get_details('2004-06-12', '2004-06-22'))

    # stock_AAOI = Stock_details('AAOI')
    # stock_AAOI.name
    # print(stock_AAOI.symbol)
    # print(stock_AAOI.name)
    # print(stock_AAOI.get_details('2004-06-12', '2005-06-22'))

    stock_AAAU = Stock_details('AAAU')
    stock_AAAU.name


## Verifying the format of output for method 'get_details' is correct:
# import json
# with open('/home/seadata/Documents/Python projects/Final project/validate_json.json', 'w') as f:
#     json.dump(stock_AA.get_details('1962-01-02', '1962-01-05'), f, ensure_ascii=False)


# with open('/home/seadata/Documents/Python projects/Final project/validate_json.json') as f:
#     data = json.load(f)
#     print(data)

