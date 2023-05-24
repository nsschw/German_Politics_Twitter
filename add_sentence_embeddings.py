import json
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
import swifter
import numpy as np


def encode_text_in_batches(df, batch_size=128):
    embeddings = []
    for i in tqdm(range(0, len(df), batch_size)):
        batch_text = df.iloc[i:i+batch_size]["Text"].tolist()
        batch_embeddings = model_sentence.encode(batch_text)
        embeddings.extend(batch_embeddings)
    return embeddings


if __name__ == '__main__':

    model_sentence = SentenceTransformer('sentence-transformers/all-mpnet-base-v2', device="cuda")
    
    print("Reading files...")
    df = pd.read_pickle("Data/Datasets/Tweets.pickle")  

    print("Encoding text...")
    df["embedding"] = encode_text_in_batches(df, batch_size=128)

    print("Saving dataframe...")
    df.to_pickle("Data/Datasets/Tweets.pickle")