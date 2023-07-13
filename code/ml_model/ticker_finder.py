import os
import pickle
from math import log
from time import time
from typing import List, Dict
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from gensim.models import FastText


class TickerFinder:
    def __init__(self, version):
        self.tfidf = None
        self.fasttext = None
        self.keywords = {}
        self.version = version

    def fit_tfidf(self, data):
        self.dct = Dictionary(data.values())
        self.bow = {
            ticker: self.dct.doc2bow(doc) for ticker, doc in data.items()
        }
        self.tfidf = TfidfModel(self.bow.values())

    def fit_fasttext(self, data):
        self.fasttext = FastText(min_n=2, max_n=4)
        self.fasttext.build_vocab(corpus_iterable=data.values(), update=False)
        self.fasttext.train(
            corpus_iterable=data.values(),
            total_examples=len(data.values()),
            epochs=5,
        )

    def fit_keywords(self, data, logn):
        if not self.tfidf:
            print("ERROR: tfidf not trained.")
            return
        for ticker in data.keys():
            keywords = self.tfidf[self.bow[ticker]]
            keywords = sorted(keywords, key=lambda x: -x[1])
            keywords = keywords[: int(log(len(keywords)) / log(logn))]
            self.keywords[ticker] = {
                self.dct[kw]: score for kw, score in keywords
            }

    def fit(
        self,
        data: Dict[str, List[str]],
        logn=1.5,
    ):
        if not self.tfidf:
            print("start training tfidf...")
            start = time()
            self.fit_tfidf(data)
            print(f"tfidf finished! time: {time() - start}")  # ~1 min
        if not self.fasttext:
            print("start training fasttext...")
            start = time()
            self.fit_fasttext(data)
            print(f"fasttext finished! time: {time() - start}")  # ~10 min
        if not self.keywords:
            print("start making keywords dict...")
            start = time()
            self.fit_keywords(data, logn)
            print(f"keywords dict finished! time: {time() - start}")  # ~10 sec
        print("train finished!")

    def score(self, keyword, ticker):
        if not self.fasttext:
            print("ERROR: fasttext not trained.")
            return "", 0
        score = 0
        for kw, tfidf_score in self.keywords[ticker].items():
            similarity = self.fasttext.wv.similarity(kw, keyword) * 2
            if similarity > 1:
                similarity **= 2
            score += similarity * tfidf_score
            if kw == keyword:
                score += 5
        return ticker, score

    def predict(self, keyword, topn=10):
        return sorted(
            (self.score(keyword, ticker) for ticker in self.keywords.keys()),
            key=lambda x: -x[1],
        )[:topn]

    def save(self, model_dir):
        model = f"{model_dir}/{self.version}.model"
        model_bin = (
            self.fasttext,
            self.keywords,
            self.dct,
            self.bow,
            self.tfidf,
        )
        if not all(model_bin):
            return print("model not trained yet")
        if os.path.exists(model):
            return print(f"{model} Already Exists")
        with open(model, "wb") as f:
            pickle.dump(model_bin, f)
            print(f"{model} saved")

    def load(self, model_dir):
        model = f"{model_dir}/{self.version}.model"
        if os.path.exists(model):
            with open(model, "rb") as f:
                (
                    self.fasttext,
                    self.keywords,
                    self.dct,
                    self.bow,
                    self.tfidf,
                ) = pickle.load(f)
                print(f"{model} loaded")
            return
        print(f"{model} not found")
        return


if __name__ == "__main__":
    tk = TickerFinder()
