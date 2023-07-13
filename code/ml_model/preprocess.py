import os
import re
import numpy as np
from konlpy.tag import Mecab # for iMac
# from mecab import MeCab  # for docker
import pandas as pd

def preprocess_ticker(ticker, data_dir='./data'):
    document = pd.read_csv(f'{data_dir}/{ticker}.csv')
    title = document['title'].apply(preprocess)
    content = document['content'].apply(preprocess)
    return np.sum(title + content)

def preprocess(doc):
    stop_words_path = "stopwords-ko.txt"
    if not preprocess.SW:
        preprocess.SW = set()
        with open(stop_words_path, encoding="utf-8") as f:
            for word in f:
                preprocess.SW.add(word.strip())
    doc = re.sub("[\{\}\[\]\/?.,;:|\)*~`!^\-_+<>@\#$%&\\\=\('\"]", " ", doc)
    tok = Mecab() # MeCab for docker
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
            "MAG"
        ]:
            words.append(word)
    return " ".join(words)
preprocess.SW = None