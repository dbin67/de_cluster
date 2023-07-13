from datetime import datetime, timedelta
import pytz
import time
import re
from mecab import MeCab

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


default_args = {
    "owner": "donghyun",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
tz = pytz.timezone("Asia/Seoul")
tickers = ["005930", "035420"]


def preprocess(doc):
    stop_words_path = "/scripts/stopwords-ko.txt"
    if not preprocess.SW:
        preprocess.SW = set()
        with open(stop_words_path, encoding="utf-8") as f:
            for word in f:
                preprocess.SW.add(word.strip())
    doc = re.sub("[\{\}\[\]\/?.,;:|\)*~`!^\-_+<>@\#$%&\\\=\('\"]", " ", doc)
    tok = MeCab()
    text_pos = [
        pair
        for pair in tok.pos(doc)
        if pair[0] not in preprocess.SW and len(pair[0]) > 1
    ]
    words = []

    for word, pos in text_pos:
        if pos in [
            "NNG",
            "NNP",
            "NNB",
            "NNBC",
            "NR",
            "NP",
            "VV",
            "VA",
            "MAG",
        ]:
            words.append(word)
    return " ".join(words)


preprocess.SW = None


def task_crawling():
    # Crawling Part

    import requests
    from bs4 import BeautifulSoup
    from hashlib import md5

    idx = Variable.get("index_ticker", -1)
    if idx == -1:
        Variable.set("index_ticker", 0)
        idx = 0
    Variable.set("index_ticker", (int(idx) + 1) % len(tickers))
    ticker = tickers[int(idx)]

    data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
    }
    sleep_interval = 1

    # enter ticker news page
    url = f"https://finance.naver.com/item/news_news.naver?code={ticker}&page={1}&sm=title_entity_id.basic&clusterId="
    time.sleep(sleep_interval)
    response = requests.get(url, headers)

    # parsing => delete related news on page 1
    soup = BeautifulSoup(response.text, "html.parser")
    all_tr = soup.select("table.type5 > tbody > tr:not(.relation_lst)")
    related_tr = soup.select(
        "table.type5 > tbody > tr.relation_lst > td > table > tbody > tr"
    )
    articles = list(set(all_tr) - set(related_tr))

    # collect links and datetimes of articles on page 1
    for article in articles:
        # link
        link = article.select("a")[0].attrs["href"]
        # date
        date = article.select("td.date")[0].text.strip()
        date = datetime.strptime(date, "%Y.%m.%d %H:%M")
        data.append([date, f"https://finance.naver.com{link}"])

    # sort by datetime
    data.sort(reverse=True)

    # last article in database
    last = Variable.get(ticker, -1)
    if idx == -1:
        Variable.set(ticker, "")
        last = ""

    # collect title and contents of articles
    for i in range(len(data)):
        # link
        url = data[i][1]

        # move to article page
        time.sleep(sleep_interval)
        response = requests.get(url, headers)
        soup = BeautifulSoup(response.text, "html.parser")

        # title and content
        title = soup.select_one("strong.c")
        content = soup.select_one("div.scr01")
        if title and content:
            title = title.text.strip()
            content = content.text.strip()
            hashed = md5(title.encode()).hexdigest()
            if hashed == last:
                data = data[:i]
                break
            data[i].append(title)
            data[i].append(content)
    if data:
        recent_title = data[0][2]
        Variable.set(ticker, md5(recent_title.encode()).hexdigest())

    print("Crawling Success")
    print(f"Crawled {len(data)} articles for {ticker}")

    # Preprocess Part
    processed_data = []
    for date, _, title, content in data:
        processed_data.append(
            (
                title,
                preprocess(title),
                content,
                preprocess(content),
                f"{date.year}{date.month:02}{date.day:02}",
            )
        )

    print("Preprocess Success")

    # HBase Part
    import happybase

    connection = happybase.Connection("thrift", port=9090)
    connection.open()

    # Load Data Part

    # get table handler for origin data
    if bytes("tickers", "utf-8") not in connection.tables():
        connection.create_table("tickers", {"cf": dict()})
    table_origin = connection.table("tickers")

    # get table handler for cleaned data
    if bytes("tickers_processed", "utf-8") not in connection.tables():
        connection.create_table("tickers_processed", {"cf": dict()})
    table_cleaned = connection.table("tickers_processed")

    # put data to table
    with table_origin.batch(batch_size=1000) as origin:
        with table_cleaned.batch(batch_size=1000) as cleaned:
            for (
                title,
                title_cleaned,
                content,
                content_cleaned,
                date,
            ) in processed_data:
                rowkey = (
                    f"{ticker[::-1]}:{date}:{md5(title.encode()).hexdigest()}"
                )
                origin.put(rowkey, {"cf:title": title, "cf:content": content})
                cleaned.put(
                    rowkey,
                    {"cf:title": title_cleaned, "cf:content": content_cleaned},
                )

    print("Load to Hbase Success")


with DAG(
    default_args=default_args,
    dag_id="crawl_clean_load",
    description="crawling from naver, clean by Mecab, load to Hbase",
    start_date=datetime(2023, 6, 19, tzinfo=tz),
    schedule_interval="@daily",
) as dag:
    task = PythonOperator(
        task_id="task_crawling",
        python_callable=task_crawling,
    )

    task
