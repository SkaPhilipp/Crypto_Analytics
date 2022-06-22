from concurrent.futures.process import _ExceptionWithTraceback
import requests

import json

import xlsxwriter
import xlrd

import pandas as pd

from neo4j import GraphDatabase

from binance import Client




# maximum rank
max_rank = 100


#####################################################################################################
#                                                                                                   #
#                             Pulling top rankings from CoinmarketCap API                           #
#                                                                                                   #
#####################################################################################################



cmk_api_url = '<cmk-api-url>'

cmk_api_key = '<cmk-api-key>'


parameters = {
  'start':'1',
  'limit':'5000',
  'convert':'USD'
}

headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': cmk_api_key,
}

session = requests.Session()
session.headers.update(headers)

try:
  response = session.get(cmk_api_url, params=parameters)
  data = json.loads(response.text)
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)


rankings = {}
if response.status_code == 200:
    data = response.json()
    for d in data['data']:
        symbol = d['symbol']
        if int(d['cmc_rank']) <= max_rank:
          rankings[d['cmc_rank']] = symbol


#####################################################################################################
#                                                                                                   #
#                             Writing rankings to Excel file                                        #
#                                                                                                   #
#####################################################################################################

# Output rankings and write to excel file

# Workbook() takes one, non-optional, argument
# which is the filename that we want to create.
workbook = xlsxwriter.Workbook('ranking.xlsx')
 
# The workbook object is then used to add new
# worksheet via the add_worksheet() method.
worksheet = workbook.add_worksheet("new")

# Header for worksheet
worksheet.write('A1', 'Rank')
worksheet.write('B1', 'Currency')

for rank in rankings:
  print(str(rank) + "  --  " + rankings[rank])
  
  # write rankings to Excel file
  worksheet.write('A' + str(rank + 1), rank)
  worksheet.write('B' + str(rank + 1), rankings[rank])

# Finally, close the Excel file
# via the close() method.
workbook.close()

'''

#####################################################################################################
#                                                                                                   #
#                             Reading rankingss from Excel file                                     #
#                                                                                                   #
#####################################################################################################



############################################# Use pandas to read xlsx file format #################################

# Read rankings from excel file

input_file = ("ranking.xls")

workbook = xlrd.open_workbook(input_file)
worksheet = workbook.sheet_by_index(0)

rankings = {}

for rank in range(1, max_rank + 1):
  rankings[int(worksheet.cell_value(rank, 0))] = worksheet.cell_value(rank, 1)



'''

#####################################################################################################
#                                                                                                   #
#                             Pulling data on trading pairs from Binance API                        #
#                                                                                                   #
#####################################################################################################




# Get market trade pairings

symbols_of_interest = []
for base in rankings.values():
  for quote in rankings.values():
    if base !=quote:
      symbols_of_interest.append(base + quote)

#print(symbols_of_interest)


bn_api_key = '<bn-api-key>'
bn_secret = '<bn-api-secret>'


# api key/secret are required for user data endpoints
client = Client(bn_api_key, bn_secret)


# Get exchange info
symbols = client.get_exchange_info()



# Get exchange information
'''
count = 1
for s in exchange_info['symbols']:
  print(str(count) + " -- " + s['symbol'])
  count += 1
'''


#symbols = pd.DataFrame(symbols["symbols"])["symbol"]

#ticker_df = pd.DataFrame(client.get_all_tickers())

#print(ticker_df)

#print(ticker_df[ticker_df['symbol'].isin(["ETHBTC","LTCBTC"])])

tickers = client.get_all_tickers()

#print(tickers)
'''
for ticker in tickers:
  if ticker['symbol'] == 'ETHBTC':
    print(ticker['price'])
'''
#print(ticker_df['symbol'])





#####################################################################################################
#                                                                                                   #
#                             Storing information in Graph Database                                 #
#                                                                                                   #
#####################################################################################################



class graph_database:


    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def reset(self):
        with self.driver.session() as session:
            session.run("MATCH (m) DETACH DELETE m")

    def close(self):
        self.driver.close()

    def create_vertex(self, symbol):
        cypher_query = "CREATE (c:Coin {symbol: '" + symbol + "'})"
        with self.driver.session() as session:
            session.run(cypher_query)

    def create_relationship(self, out_vertex, in_vertex, value):
        cypher_query = "MATCH (c1:Coin), (c2:Coin) WHERE c1.symbol = '"+ out_vertex + "' AND c2.symbol = '"+ in_vertex + "' CREATE (c1)-[:TRADE {value:"+ str(value) + "}]->(c2)"
        with self.driver.session() as session:
            session.run(cypher_query)

    def execute_query(self, query):
        with self.driver.session() as session:
            session.run(query)

    def execute_query_with_output_result(self, query):
        with self.driver.session() as session:
            record = session.run(query)
            for i in record:
              line = dict(i)
              print(dict(i))
              #print(dict(i)['symbol'] + "  --  " + str(dict(i)['score']))
            return record
    
    def execute_page_rank(self, graph_name):
        projection = "CALL gds.graph.project('"+ graph_name +"','Coin','TRADE',{relationshipProperties: 'value'})"
        self.execute_query(projection)
        page_rank = "CALL gds.pageRank.stream('"+ graph_name +"') YIELD nodeId, score RETURN gds.util.asNode(nodeId).symbol AS symbol, score ORDER BY score DESC, symbol ASC"
        self.execute_query_with_output_result(page_rank)


#####################################################################################################
#                                                                                                   #
#                             Main function to execute PageRank                                     #
#                                                                                                   #
#####################################################################################################


def main():

    #local bolt and http port, etc:
    local_bolt = '<neo4j-local-bolt>'
    local_http = '<neo4j-local-http>'
    local_pw = '<neo4j-pw>'
    local_user = "neo4j"

    coin_db = graph_database(local_bolt, local_user, local_pw)

    # reseting graph db
    coin_db.reset()

    # creating currecncy vertices
    for currency in rankings.values():
      coin_db.create_vertex(currency)
  
    # creating currecncy relationships
    for base in rankings.values():
      for quote in rankings.values():
        for ticker in tickers:
          if ticker['symbol'] == base + quote:
            coin_db.create_relationship(base, quote, ticker['price'])

    # return result from PageRank Algorithm

    graph_name = 'new'
    result = coin_db.execute_page_rank(graph_name)
    
    # find out why execute_page_rnak does not return result correctly, yet if printed in execute_query it works
    #for record in result:
    #  print(record)

    # closing graph db
    coin_db.close()


main()
