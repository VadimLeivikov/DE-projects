import pandas as pd
from  json import loads, dumps
from stock_details import Stock_details
import logs_writer
import os
from dotenv import load_dotenv
load_dotenv("/home/seadata/Documents/Python projects/Final project/config.env")

## Creating json-list of stock-symbols:
path_symbols = os.getenv('PATH_SYMBOLS')
symbols = pd.read_csv(path_symbols)['Symbol']
result = symbols.to_json(orient="records")
parsed = loads(result)
json_symbols = dumps(parsed, indent=4)


## Inheritating the class Stock_details end enriching it:
class Stock_stats(Stock_details):
    def __init__(self, symbol):
        super().__init__(symbol)
    def dates_range(self):
        from sqlalchemy import select, func
        max_fn = func.max(self._Stock_details__stock_rates.c.Date)
        min_fn = func.min(self._Stock_details__stock_rates.c.Date)
        range = select(max_fn, min_fn)\
                .where(self._Stock_details__stock_rates.c.Symbol  == self.symbol)
        result = self._Stock_details__conn.execute(range)
        self.__max_date, self.__min_date = result.fetchone()
        return self.__max_date, self.__min_date
    
    @logs_writer.log_function_call
    def aggregations(self, start_date:str, end_date:str):
        import pandas as pd
        import json
        from datetime import date, timedelta
        from sqlalchemy import select, desc, and_

        self.dates_range()

        try:
            starting_price =  select(self._Stock_details__stock_rates.c.Open).where(self._Stock_details__stock_rates.c.Date == date.fromisoformat(start_date))
            result = self._Stock_details__conn.execute(starting_price)
            starting_price = float(result.fetchone()[0])
        except Exception as e:
            logs_writer.logs(9, type(self).__name__, start_date)
            print(f'Oops: there is no data in the "stock_rates" table under the selected start_date ({start_date}), choose another one start_date: ', str(e))
            
            ## Defining the closest dates with existing data:
            if start_date > self.__max_date: 
                start_date_up = self.__max_date
            else:
                start_date_up = select(self._Stock_details__stock_rates.c.Date)\
                            .distinct()\
                            .where(
                                 and_(self._Stock_details__stock_rates.c.Date > date.fromisoformat(start_date)),
                                 (self._Stock_details__stock_rates.c.Symbol  == self.symbol)
                                 )\
                            .order_by(self._Stock_details__stock_rates.c.Date)\
                            .limit(1)
            result = self._Stock_details__conn.execute(start_date_up)
            start_date_up = result.fetchone()[0]

            if start_date < self.__min_date: 
                start_date_down = self.__min_date
                print(start_date_down)
            else:     
                start_date_down = select(self._Stock_details__stock_rates.c.Date)\
                                .distinct()\
                                .where(
                                    and_(self._Stock_details__stock_rates.c.Date < date.fromisoformat(start_date)),
                                    (self._Stock_details__stock_rates.c.Symbol  == self.symbol)
                                    )\
                                .order_by(desc(self._Stock_details__stock_rates.c.Date))\
                                .limit(1)
                result = self._Stock_details__conn.execute(start_date_down)
                start_date_down = result.fetchone()[0]

            if start_date_down == start_date_up: print(f'The closest date populated with data is: {start_date_down}')
            else: print(f'The closest dates populated with data are: {start_date_down} and {start_date_up}')
 
            return None

        try:
            ending_price = select(self._Stock_details__stock_rates.c.Close).where(self._Stock_details__stock_rates.c.Date == date.fromisoformat(end_date))
            result = self._Stock_details__conn.execute(ending_price)
            ending_price = float(result.fetchone()[0])
        except Exception as e:
            logs_writer.logs(10, type(self).__name__, end_date)
            print(f'Oops: there is no data in the "stock_rates" table under the selected end_date ({end_date}), choose another one end_date: ', e)

            ## Defining the closest dates with existing data:
            if end_date > self.__max_date: 
                end_date_up = self.__max_date
            else:
                end_date_up = select(self._Stock_details__stock_rates.c.Date)\
                                .distinct()\
                                .where(
                                    and_(self._Stock_details__stock_rates.c.Date > date.fromisoformat(end_date),
                                    self._Stock_details__stock_rates.c.Symbol  == self.symbol)
                                    )\
                                .order_by(self._Stock_details__stock_rates.c.Date)\
                                .limit(1)
                result = self._Stock_details__conn.execute(end_date_up)
                end_date_up = result.fetchone()[0]

            if end_date < self.__min_date: 
                end_date_down = self.__min_date
            else:
                end_date_down = select(self._Stock_details__stock_rates.c.Date)\
                            .distinct()\
                            .where(
                                 and_(self._Stock_details__stock_rates.c.Date < date.fromisoformat(end_date)),
                                 (self._Stock_details__stock_rates.c.Symbol  == self.symbol)
                                 )\
                            .order_by(desc(self._Stock_details__stock_rates.c.Date))\
                            .limit(1)
                result = self._Stock_details__conn.execute(end_date_down)
                end_date_down = result.fetchone()[0]

            if end_date_down == end_date_up: print(f'The closest date populated with data is: {end_date_down}')
            else: print(f'The closest dates populated with data are: {end_date_down} and {end_date_up}')
 
            return None
        print('starting_price: ', starting_price, type(starting_price))
        print('ending_price: ', ending_price, type(ending_price))


        query  = '''
                    SELECT Symbol, Company_name, MAX(High), MIN(Low), AVG(Close)
                    FROM stock_rates
                    WHERE (Symbol = %(dynamic_symbol)s OR Company_name = %(dynamic_company_name)s)
                            AND (Date BETWEEN %(dynamic_start)s AND %(dynamic_end)s)
                    GROUP BY Symbol, Company_name
                    '''
        df_agg = pd.read_sql(query, con = self._Stock_details__engine, params={"dynamic_symbol": self.symbol\
                                                                     , 'dynamic_company_name':self._Stock_details__company_name\
                                                                        ,'dynamic_start':date.fromisoformat(start_date)\
                                                                            ,'dynamic_end':date.fromisoformat(end_date)})
        df_agg['Yield'] = ((ending_price - starting_price)/starting_price)*100
        df_agg.rename(columns={'MAX(High)': 'MAX Rate', 'MIN(Low)': 'MIN Rate', 'AVG(Close)': 'AVG Rate'}, inplace = True) 
        result = df_agg.to_json(orient="records")
        parsed = json.loads(result)
        print('self.symbol: ', self.symbol)
        with open(f'stock_stats_{self.symbol}.json', 'w') as f:
             json.dump(parsed, f, ensure_ascii=False)
        return json.dumps(parsed, indent=4)




## Self-checking:

## Initializing an instance of the Stocks_stats class and checking it's methods:
stock_AAAU = Stock_stats('AAAU')
# print(stock_AAAU.symbol)
# print(stock_AAAU.name)
# print(stock_AAAU.get_details('1962-01-02', '1962-01-05'))
# print(stock_AAAU.aggregations('1962-01-02', '1962-01-07'))


# Initializing an empty dictionary:
# stocks = {}
# Here we are creating the dictionary of instances:
# start_date = '1999-12-01'
# end_date = '1999-12-05'  
# 1999-12-06 1962-01-08
i = 0
# for symbol in loads(json_symbols)[:2]:
#     print()
#     print('current stock: ', symbol)
#     # start_date = input('enter start_date: ')
#     # end_date = input('enter end_date: ')
#     stocks[f'{symbol}'] = Stock_stats(symbol)
#     stocks[f'{symbol}'].name
#     print(stocks[f'{symbol}'].aggregations(start_date, end_date))

#     i +=1
#     if i >0: break


# start_date = '1999-12-01'
# end_date = '1999-12-08'  
# ## 1999-12-06 1962-01-08
# i = 0
# for symbol in loads(json_symbols)[:20]:
#     print()
#     print('current stock: ', symbol)
#     # start_date = input('enter start_date: ')
#     # end_date = input('enter end_date: ')
#     stocks[f'{symbol}'] = Stock_stats(symbol)
#     stocks[f'{symbol}'].name
#     print(stocks[f'{symbol}'].aggregations(start_date, end_date))

#     i +=1
#     if i >10: break




