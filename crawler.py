import requests # para requisições http
# import json # para gerar JSON a partir de objetos do Python
from bs4 import BeautifulSoup # BeautifulSoup é uma biblioteca Python de extração de dados de arquivos HTML e XML.
import xml.etree.ElementTree as ET
import pandas as pd
from random import shuffle
from datetime import datetime
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor


QTD_BASE = 10


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
        data = row.text.strip().split('')
        companies.append({'TICKER': data[0], 'NAME': data[1]})
    
    return companies


def get_news(url: str):
    page = requests.get(url)
    xml_content = page.content
    root = ET.fromstring(xml_content)

    # Extrair o conteúdo das tags "loc"
    locs = root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
    lastmod = root.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')

    locs = [locs[i] for i in random_numbers(len(locs), QTD_BASE)]

    news = []
    for loc in locs:
        url = loc.text
        if '.xml' in url:
            print(f'Extraindo urls de {url}')
            news += get_news(url)
        else:
            page = requests.get(url)
            parsed_content = BeautifulSoup(page.content, 'html.parser')
            article = parsed_content.find('article')

            date = datetime.strptime(lastmod.text[:10], '%Y-%m-%d')
            news += [{'URL': url, 'DATE': date, 'NEWS': article.text}]
    
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
    
    # Obter resultados
    data = []
    for result in results:
        news_mentions = result.result()
        if news_mentions['MENTIONS']:
            article, mentions = news_mentions['ARTICLE'], news_mentions['MENTIONS']
            for mention in mentions:
                data.append({'DATE': article['DATE'], 'URL': article['URL'],
                            'NAME': mention['NAME'], 'TICKER': mention['TICKER']})
    
    return pd.DataFrame(data)


companies = get_companies_data()
df_companies = pd.DataFrame(companies)

news = get_news('https://valor.globo.com/sitemap/valor/sitemap.xml')
df_news = pd.DataFrame(news)

# Processar as notícias em paralelo
df_result = news_processing(df_news, df_companies)



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
    stock = yf.download(tickers=ticker)
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

df_result['OPEN'] = opens
df_result['CLOSE'] = closes
df_result['VARIATION'] = variations

df_result.to_csv('result.csv')
