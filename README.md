Desafio semantix
----------------
Desafio proposto pela empresa semantix para gerar um crawler para 3 páginas:

-https://m.investing.com/currencies/usd-brl-historical-data

-https://www.investing.com/equities/StocksFilter?index_id=20

-http://www.investing.com/equities/StocksFilter?index_id=17920'

Arquivos e diretórios relevantes
---------------------------------
desafioSematixCrawler/desafiosemantix/spiders/stock_market_spider.py: Arquivo onde o parse e as persistencias são feitos.
desafioSematixCrawler/stock_market_data.db: Exemplo de arquivo de banco gerado com o crawler.
desafioSematixCrawler/desafiosemantix/generated_files/: Diretório de arquivos csv gerados com o crawlwer.


Pré-requisitos para rodar o report:
----------------------------------
1) Pycharm
2) pacote crawler instalado

Pré-requisitos para rodar o report:
----------------------------------
1) Pycharm
2) pacote crawler instalado

Para rodar a aplicação
----------------------
1)Importar o diretorio desafioSemantix
2)Na linha de comando digitar scrapy crawl stockmarketspider -L WARNING -a basicpath <nome do diretorio onde os arquivos .csv ficarão>
Obs: O programa possui um bug que nao foi resolvido que é fazer o scrawler das url em ordem ( a 
url https://m.investing.com/currencies/usd-brl-historical-data tem de ser processada primeiro) caso a mensagem de erro 
'ERROR: Conversion data is not avaiable. Please run again the program' apareça apenas rode de novo o programa.

Para ver os dados extraidos das páginas num banco de dados
----------------------------------------------------------
1) Vá a página https://sqliteonline.com/
2) Importe o arquivo stock_market_data.db
3) Digite as queries:
-SELECT * FROM bovespa_data;
-SELECT * FROM dollar_real_data;
-SELECT * FROM dollar_real_data;

Bugs conhecidos:
----------------
O programa não consegue processar as queries na ordem desejada. Caso a mensagem de erro 
'ERROR: Conversion data is not avaiable. Please run again the program' apareça apenas rode de novo o programa.

Features fantantes:
-------------------
O requerimento de rodar em docker ficou faltando por falta de tempo do candidato.
