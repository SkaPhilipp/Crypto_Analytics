from concurrent.futures.process import _ExceptionWithTraceback
import requests

import json

import xlsxwriter
import xlrd

import matplotlib.pyplot as plt

from neo4j import GraphDatabase

from binance import Client


# maximum ranking of the top <max_rank> performing currencies
max_rank = 100


####################################################################################################
#                                                                                                  #
#                             Pulling top rankings from CoinmarketCap API                          #
#                                                                                                  #
####################################################################################################


# set cmk api url and key
cmk_api_url = '<cmk-api-url>'
cmk_api_key = '<cmk-api-key>'

# set parameters for api request
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

# perform api request
try:
  response = session.get(cmk_api_url, params=parameters)
  data = json.loads(response.text)
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)

# store information on currencies and ranking in dictionary
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

# Create new workbook
workbook = xlsxwriter.Workbook('ranking.xlsx')
 
# Add worksheet to workbook
worksheet = workbook.add_worksheet("new")

# Header for worksheet
worksheet.write('A1', 'Rank')
worksheet.write('B1', 'Currency')

for rank in rankings:
  # Print top rankings
  print(str(rank) + "  --  " + rankings[rank])
  
  # write rankings to Excel file
  worksheet.write('A' + str(rank + 1), rank)
  worksheet.write('B' + str(rank + 1), rankings[rank])

# close excel file
workbook.close()

'''

#####################################################################################################
#                                                                                                   #
#                             Reading rankingss from Excel file                                     #
#                                                                                                   #
#####################################################################################################



# Reading Excel File only necessary if running multiple times within short period and don't want to use up all api credits

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

# Set api key and api secret
bn_api_key = '<bn-api-key>'
bn_secret = '<bn-api-secret>'


# api key/secret are required for user data endpoints
client = Client(bn_api_key, bn_secret)

# Get data on trading pairs
tickers = client.get_all_tickers()




#####################################################################################################
#                                                                                                   #
#                             Storing information in Graph Database                                 #
#                                                                                                   #
#####################################################################################################


# Define graph database class
class graph_database:

    # Initialise graph databse using Neo4j api
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    # Reset graph database by deleting all nodes and relationships
    def reset(self):
        with self.driver.session() as session:
            session.run("MATCH (m) DETACH DELETE m")

    # Close graph database
    def close(self):
        self.driver.close()

    # Create new vertex for each currency
    def create_node(self, symbol):
        cypher_query = "CREATE (c:Coin {symbol: '" + symbol + "'})"
        with self.driver.session() as session:
            session.run(cypher_query)

    # Create edge for each trading pair with data on trades
    def create_relationship(self, out_vertex, in_vertex, value):
        cypher_query = "MATCH (c1:Coin), (c2:Coin) WHERE c1.symbol = '"+ out_vertex + "' AND c2.symbol = '"+ in_vertex + "' CREATE (c1)-[:TRADE {value:"+ str(value) + "}]->(c2)"
        with self.driver.session() as session:
            session.run(cypher_query)

    # Execute Cypher query
    def execute_query(self, query):
        with self.driver.session() as session:
            session.run(query)

    # Execute Cypher query and return result
    def execute_query_with_output_result(self, query):
        with self.driver.session() as session:
          result = session.run(query)
          return [dict(i) for i in result]

    # Execute PageRank algorithm
    def execute_page_rank(self, graph_name):
        projection = "CALL gds.graph.project('"+ graph_name +"','Coin','TRADE',{relationshipProperties: 'value'})"
        self.execute_query(projection)
        page_rank = "CALL gds.pageRank.stream('"+ graph_name +"') YIELD nodeId, score RETURN gds.util.asNode(nodeId).symbol AS symbol, score ORDER BY score DESC, symbol ASC"
        result = self.execute_query_with_output_result(page_rank)
        return result


#####################################################################################################
#                                                                                                   #
#                             Main function to execute PageRank                                     #
#                                                                                                   #
#####################################################################################################


def main():

    # Local bolt and http port, etc:
    local_bolt = '<neo4j-local-bolt>'
    local_http = '<neo4j-local-http>'
    local_pw = '<neo4j-pw>'
    local_user = "neo4j"

    coin_db = graph_database(local_bolt, local_user, local_pw)

    # reseting graph database
    coin_db.reset()

    # creating currecncy vertices
    for currency in rankings.values():
      coin_db.create_node(currency)
  
    # creating currecncy relationships
    for base in rankings.values():
      for quote in rankings.values():
        for ticker in tickers:
          if ticker['symbol'] == base + quote:
            coin_db.create_relationship(base, quote, ticker['price'])

    # return result from PageRank Algorithm

    graph_name = 'new'
    page_ranks = coin_db.execute_page_rank(graph_name)

    # store rankings and symbols in lists
    symbols = []
    scores = []

    for result in page_ranks:
      symbols.append(result['symbol'])
      scores.append(round(result['score'],5))

    
    # creating a bar chart with top 10 scores for respective currencies
    x_values = list(symbols[:10])
    y_values = list(scores[:10])
    
    plt.bar(x_values, y_values, color ='red', width = 0.6)
    plt.xlabel("Top currencies (with respect to PageRank)")
    plt.ylabel("PageRank scores")
    plt.show()

    # closing graph db
    coin_db.close()


main()
