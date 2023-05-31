import requests # para requisições http
from bs4 import BeautifulSoup # BeautifulSoup é uma biblioteca Python de extração de dados de arquivos HTML e XML.
import xml.etree.ElementTree as ET
import pandas as pd
from random import shuffle
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor

from time import sleep
import json
import codecs


QTD_BASE = 3
SLEEP_TIME = 5


# Seleciona valores aleatórios
def random_numbers(n: int, list_size: int):
    numbers = list(range(n))
    shuffle(numbers)
    return numbers[:list_size]


def get_companies_data():
    page = requests.get('https://www.dadosdemercado.com.br/bolsa/acoes')
    parsed_content = BeautifulSoup(page.content, 'html.parser')
    data = parsed_content.find_all('tr')
    
    companies = []
    for row in data[1:]:
        data = row.text.strip().splitlines()
        companies.append({'TICKER': data[0], 'NAME': data[1]})
    
    return companies


def get_news(url: str):
    try:
        page = requests.get(url)
    except Exception as e:
        print(e)

    xml_content = page.content
    root = ET.fromstring(xml_content)

    # Extrair o conteúdo das tags "loc", que contém os links das notícias
    locs = root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
    
    # Extrair o conteúdo das tags "loc", que contém a data das notícias
    lastmod = root.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')

    # Seleciona valores aleatórios para que as notícias sejam bem distribuídas no tempo
    locs = [locs[i] for i in random_numbers(len(locs), QTD_BASE)]

    news = []
    for loc in locs:
        url = loc.text
        if '.xml' in url:
            print(f'Extraindo urls de {url}')
            news += get_news(url)
        else:
            sleep(SLEEP_TIME)
            try:
                page = requests.get(url)
            except Exception as e:
                print(e)

            parsed_content = BeautifulSoup(page.content, 'html.parser')
            title = parsed_content.find('h1', {'class' : 'content-head__title'})
            article = parsed_content.find('article')
            date = lastmod.text[:10]

            news += [{'URL': url, 'DATE': date, 'TITLE': title.text, 'NEWS': article.text}]
    
    return news


# Função para verificar menção de empresa em uma notícia
def check_mention_company(article, companies):
    mentions = []
    for _, company in companies.iterrows():
        if company['TICKER'] in article['NEWS'] or company['NAME'] in article['NEWS']:
            mentions.append(company)
    return {'ARTICLE': article, 'MENTIONS': mentions}


# Função para processamento paralelo das notícias
def news_processing(news, companies):
    results = []
    with ThreadPoolExecutor() as executor:
        for _, article in news.iterrows():
            future = executor.submit(check_mention_company, article, companies)
            results.append(future)
    
    # Extrair dados
    data = []
    for result in results:
        news_mentions = result.result()
        if news_mentions['MENTIONS']:
            article, mentions = news_mentions['ARTICLE'], news_mentions['MENTIONS']
            for mention in mentions:
                data.append({'DATE': article['DATE'], 'URL': article['URL'], 'TITLE': article['TITLE'],
                            'NAME': mention['NAME'], 'TICKER': mention['TICKER']})
    
    return pd.DataFrame(data)


# Pega informações das empresas listadas na bolsa (nome e tickers)
companies = get_companies_data()
df_companies = pd.DataFrame(companies)

news = get_news('https://valor.globo.com/sitemap/valor/sitemap.xml')
df_news = pd.DataFrame(news)

# Processa as notícias em paralelo
df_result = news_processing(df_news, df_companies)


#################################################


# Pesquisar valores das ações no dia das notícias
def ticker_exists(ticker):
    try:
        yf.Ticker(ticker).info
        return True
    except:
        return False

opens = []
closes = []
variations = []
def set_values(ticker):
    sleep(SLEEP_TIME)
    try:
        stock = yf.download(tickers=ticker)
    except Exception as e:
        print(e)

    stock_data = stock[date:date]
    if stock_data is None or stock_data.empty or len(stock_data) == 0:
        opens.append(-1)
        closes.append(-1)
        variations.append(-1)
    else:
        op, cl = stock_data['Open'].iloc[0], stock_data['Close'].iloc[0]
        opens.append(round(op, 2))
        closes.append(round(cl, 2))
        variations.append(round((cl - op), 2))

for _, row in df_result.iterrows():
    ticker, date = row['TICKER'], row['DATE']
    if ticker_exists(ticker):
        set_values(ticker)
    elif ticker_exists(f'{ticker}.SA'):
        set_values(f'{ticker}.SA')
    else:
        opens.append(-1)
        closes.append(-1)
        variations.append(-1)

# Insere os dados obtidos no DataFrame
df_result['OPEN'] = opens
df_result['CLOSE'] = closes
df_result['VARIATION'] = variations

# Converte o DataFrame para um formato mais adequado
dict_list = df_result.to_dict(orient='records')

# Converte os objetos Pyhton em objeto JSON e exporta para o noticias.json
with codecs.open('resultado.json', 'w', encoding='utf-8') as arquivo:
  arquivo.write(str(json.dumps(dict_list, indent=4, ensure_ascii=False)))
