# -*- coding: utf-8 -*-
"""

@author: jdifr

"""

import pandas as pd
import re

def remove_punctuation(series):
    return(series
                 .str.replace('.', '')
                 .str.replace(',', '')
                 .str.replace(r'\s\s+', ' ', regex=True)
                 .str.strip())

def normalize(series):
    return remove_punctuation(series.str.lower())

def last_word_counts(series):
    return (series
                  .str
                  .replace(r'^.+\s', '')
                  .value_counts())

def word_counts(series):
    return (series
                  .str
                  .split()
                  .explode()
                  .value_counts())

def drop_words_from_string(string, list):
    pat = '|'.join([r'\b'+word+r'\b' for word in list])
    string = re.sub(pat, '', string)
    string = re.sub(r'\s\s+', ' ', string)
    string = string.strip()
    return string

def drop_words_from_series(series, list):
    return series.apply(drop_words_from_string, list=list)
