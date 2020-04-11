import scrapy
import pandas as pd
import re
import sqlite3
from scrapy import Selector, Request

# classe principal para realizar o crawler dos websites:
# https://m.investing.com/currencies/usd-brl-historical-data
# https://www.investing.com/equities/StocksFilter?index_id=17920
# https://www.investing.com/equities/StocksFilter?index_id=20
class StockMarketSpider(scrapy.Spider):
    name = 'stockmarketspider'
    start_urls = [
        'https://www.investing.com/equities/StocksFilter?index_id=17920',
        'https://www.investing.com/equities/StocksFilter?index_id=20',
        'https://m.investing.com/currencies/usd-brl-historical-data'
    ]

    # Flag utilizada para verificar se dados de conversão real/dollar foram
    # carregadas
    proceed = True

    # Estrutura de dados para armazenar dados do site
    # https://m.investing.com/currencies/usd-brl-historical-data
    dollar_value_column_names = ['currency', 'value', 'change', 'perc', 'timestamp']
    dollar_value_data_frame = pd.DataFrame(columns=dollar_value_column_names)

    # Estrutura de dados para armazenar dados do site
    # https://www.investing.com/equities/StocksFilter?index_id=17920
    bovespa_value_column_names = ['name', 'last_rs', 'high_rs','low_rs', 'last_usd', 'high_usd', 'low_usd', 'chg', 'chg_perc', 'vol', 'time']
    bovespa_value_data_frame = pd.DataFrame(columns=bovespa_value_column_names)

    # Estrutura de dados para armazenar dados do site
    # https://m.investing.com/currencies/usd-brl-historical-data
    nasdaq_value_column_names = ['name', 'last_rs', 'high_rs','low_rs', 'last_usd', 'high_usd', 'low_usd', 'chg', 'chg_perc', 'vol', 'time']
    nasdaq_value_data_frame = pd.DataFrame(columns=nasdaq_value_column_names)

    # Função utilizada para realizar o parser dos dados de data
    def parse_date(self, data_info):
        date_parsed = re.split(',', data_info)[0].split()
        date_return = ""
        if(date_parsed[0] == 'Jan'):
            date_return = date_parsed[1] + "/" + "01"
        if (date_parsed[0] == 'Feb'):
            date_return = date_parsed[1] + "/" + "02"
        if (date_parsed[0] == 'Mar'):
            date_return = date_parsed[1] + "/" + "03"
        if (date_parsed[0] == 'Apr'):
            date_return = date_parsed[1] + "/" + "04"
        if(date_parsed[0] == 'May'):
            date_return = date_parsed[1] + "/" + "05"
        if(date_parsed[0] == 'Jun'):
            date_return = date_parsed[1] + "/" + "06"
        if(date_parsed[0] == 'Jul'):
            date_return = date_parsed[1] + "/" + "07"
        if (date_parsed[0] == 'Aug'):
            date_return = date_parsed[1] + "/" + "08"
        if (date_parsed[0] == 'Sep'):
            date_return = date_parsed[1] + "/" + "09"
        if (date_parsed[0] == 'Oct'):
            date_return = date_parsed[1] + "/" + "10"
        if (date_parsed[0] == 'Nov'):
            date_return = date_parsed[1] + "/" + "11"
        if (date_parsed[0] == 'Dec'):
            date_return = date_parsed[1] + "/" + "12"
        return date_return

    # Função utilizada para converter de dólar para real
    def convert_to_real(self, dollar_value):
        if (not self.dollar_value_data_frame.empty):
            dollar_real_converter = float(self.dollar_value_data_frame[self.dollar_value_data_frame['timestamp']=='10/04'].iloc[0].value)
            return (float(dollar_value.replace(",","")) * dollar_real_converter )
        else:
            print("Error: Value for conversion is not avaiable")

    # Função utilizada para fazer o parser e armazenar os dados dos sites:
    # https://www.investing.com/equities/StocksFilter?index_id=20
    # https://m.investing.com/currencies/usd-brl-historical-data
    def parse_stock_market_data(self, response):

        # Dados do site https://m.investing.com/currencies/usd-brl-historical-data
        # precisam estar disponíveis
        if(self.dollar_value_data_frame.empty):
            print("ERROR: Conversion data is not avaiable. Please run again the program")
            self.proceed = False
            return
        print("Processing data for url: " + str(response))

        # Conecta no banco de dados
        conn = sqlite3.connect('stock_market_data.db')

        # Recupera dados do site
        sel = Selector(response)
        rows_name = sel.xpath("//table[contains(@id, 'cross_rate_markets_stocks_1')]/tbody/tr/td/a")\
        .css('a').xpath('descendant-or-self::a/text()').extract()
        rows_data = sel.xpath('//table[contains(@id, "cross_rate_markets_stocks_1")]').css('tr').xpath(
        '//descendant-or-self::td/text()').extract()

        # Faz parser dos dados
        i = 0
        j = 0
        all_data = []
        while (i< len(rows_name)):
            basic_info = []
            basic_info.append(rows_name[i])
            basic_info.append(float(rows_data[j].replace(",","")))
            basic_info.append(self.convert_to_real(rows_data[j]))
            basic_info.append(float(rows_data[j+1].replace(",","")))
            basic_info.append(self.convert_to_real(rows_data[j+1]))
            basic_info.append(float(rows_data[j+2].replace(",","")))
            basic_info.append(self.convert_to_real(rows_data[j+2]))
            basic_info.append(rows_data[j+3])
            basic_info.append(rows_data[j+4])
            basic_info.append(rows_data[j+5])
            basic_info.append(rows_data[j+6])
            j = j + 7
            i = i + 1
            all_data.append(basic_info)

        # Armazena dados em arquivo e no banco de dados
        if(response.url == 'https://www.investing.com/equities/StocksFilter?index_id=17920'):
            self.bovespa_value_data_frame = pd.DataFrame(all_data, columns=self.bovespa_value_column_names)
            self.bovespa_value_data_frame.to_csv(index=False)
            self.bovespa_value_data_frame.to_csv(r'' + self.basicpath + "bovespa_values.csv",
                                                 index=False)
            self.bovespa_value_data_frame.to_sql(name="bovespa_data", con=conn, if_exists='replace', index=False)

        if(response.url == 'https://www.investing.com/equities/StocksFilter?index_id=20'):
            self.nasdaq_value_data_frame = pd.DataFrame(all_data, columns=self.nasdaq_value_column_names)
            self.nasdaq_value_data_frame.to_csv(r'' +self.basicpath+"nasdaq_values.csv",
                                               index=False)
            self.nasdaq_value_data_frame.to_sql(name="nasdaq_data", con=conn, if_exists='replace', index=False)
        conn.close()

    # Faz o parser da url
    # https://m.investing.com/currencies/usd-brl-historical-data
    def parse_dolar_value_data(self, response):

        # Esse parser tem de rodar primeiro para que os outros funcionem
        if(not self.proceed):
            return

        print("Processing dollar data for url: " + str(response))
        # Conecta no banco de dados e recupera dados da tabela da página
        conn = sqlite3.connect('stock_market_data.db')
        rows = response.css('table.scrollTbl tr').xpath('//descendant-or-self::td/text()').extract()

        # Faz parser dos dados
        i = 0
        all_data = []
        while (i < len(rows)):
            j = 0
            row_info = []
            while (j < 6):
                row_info.append(rows[i])
                j = j + 1
                i = i + 1
            row_info_ordered = []
            row_info_ordered.append('USD/BRl')
            row_info_ordered.append(row_info[1])
            row_info_ordered.append(row_info[5])
            row_info_ordered.append(row_info[5])
            row_info_ordered.append(self.parse_date(row_info[0]))
            all_data.append(row_info_ordered)

        # Insere dados em no data frame e no banco de dados
        self.dollar_value_data_frame = pd.DataFrame(all_data, columns=['currency', 'value', 'change', 'perc', 'timestamp'])
        self.dollar_value_data_frame.to_csv(
            r''+self.basicpath+'\dollar_values.csv',
            index=False)
        self.dollar_value_data_frame.to_sql(name="dollar_real_data", con=conn, if_exists='replace', index=False)
        conn.close()

    # Inicia o processo de recuperação de dados parsing e persistencia
    def parse(self, response):
        url = 'https://m.investing.com/currencies/usd-brl-historical-data'
        yield Request(url=url, callback=self.parse_dolar_value_data, priority=1)

        url = 'https://www.investing.com/equities/StocksFilter?index_id=20'
        yield Request(url=url, callback=self.parse_stock_market_data, priority=10)

        url = 'https://www.investing.com/equities/StocksFilter?index_id=17920'
        yield Request(url=url, callback=self.parse_stock_market_data, priority=10)
