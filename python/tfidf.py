# -*- coding: utf-8 -*-
"""

@author: jdifr

Creates tfidf algorithm in order to compare two string columns and outputs match scores.

"""
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd
import re


def _create_ngrams(string: str, min_length=3, max_length=3):
    '''
    returns all consecutive characters of string between min_length and max_length
    that don't contain spaces
    '''
    result = []

    for n in range(min_length, max_length+1):
        ngrams = zip(*[string[i:] for i in range(n)])
        ngrams = [''.join(ngram)for ngram in ngrams if ' ' not in ngram]
        result.extend(ngrams)
    return(result)

def tfidf_match(From: pd.Series, To: pd.Series, analyzer=_create_ngrams):
    '''
    find the best tfidf matches from From to To using analyzer to create tokens
    '''
    matches = pd.DataFrame({'From':From})

    vectorizer = TfidfVectorizer(min_df=1, analyzer=analyzer).fit(From.to_list() + To.tolist())

    def create_matrix(series):
        return pd.DataFrame(
            vectorizer.transform(series.to_list()).todense(),
            columns=sorted(vectorizer.vocabulary_)
        )

    From_matrix = create_matrix(From)
    To_matrix = create_matrix(To)

    similarity_matrix = pd.DataFrame(cosine_similarity(From_matrix, To_matrix))

    def get_max_indices(row):
        max = row.max()
        if max ==0:
            return 0
        else:
            return np.flatnonzero(row==max)

    matches['Score'] = similarity_matrix.max(axis=1).to_list()
    matches['To Index'] = similarity_matrix.apply(get_max_indices, axis=1).to_list()
    matches = matches.explode('To Index')
    matches['To'] = matches.apply(lambda row: To.iloc[row['To Index']], axis=1)

    return matches

def join_on_tfidf(left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str):
    '''
    join left and right with the best tfidf match of left[left_on] with right[right_on]
    '''

    matches = tfidf_match(left[left_on], right[right_on])
    df = left.join(matches)
    df['From Index'] = df.index
    df = pd.merge(df, right, left_on='To Index', right_on=np.arange(len(right)))
    return df
