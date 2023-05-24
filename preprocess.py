import os
import pandas as pd
import json
import spacy
import bertopic
from bertopic import BERTopic
from germansentiment import SentimentModel
import matplotlib.pyplot as plt
import datetime
import re
import pickle
from collections import defaultdict
import requests
from time import sleep
import numpy as np
import re

#JSON öffnen und zusammenfügen
def öffnen(file):

    daten = [json.loads(line) for line in open(f"Data/Tweets/{file}", "r", encoding="utf-8")]
    
    df = pd.DataFrame()
    
    x = 0
    
    for tweet in daten:      
        
        df.at[x, "Datum"] = tweet["created_at"]
        df.at[x, "Text"] = tweet["text"]
        df.at[x, "Author-ID"] = tweet["author_id"]
        df.at[x, "Likes"] = tweet["public_metrics"]["like_count"]
        df.at[x, "Retweets"] = tweet["public_metrics"]["retweet_count"]
        df.at[x, "Replies"] = tweet["public_metrics"]["reply_count"]
        df.at[x, "Quotes"] = tweet["public_metrics"]["quote_count"]
        df.at[x, "Sprache"] = tweet["lang"]
        df.at[x, "Tweet-ID"] = tweet["id"]

        try:
            df.at[x, "Impressions"] = tweet["public_metrics"]["impression_count"]
        except:
            df.at[x, "Impressions"] = 0
            
        if tweet["text"][:2] == "RT":
            RT_MUSTER = re.compile(r"(?<=RT @)[\w]*")
            RT_von  = RT_MUSTER.search(tweet["text"])
            df.at[x, "RT_from"] = RT_von.group()
        else:
            df.at[x, "RT_from"] = "Keine"
        
        try:
            df.at[x, "Reply_To"] = tweet["in_reply_to_user_id"]
        except:
            df.at[x, "Reply_To"] = "Keine"

        try: 
            if tweet["referenced_tweets"][0]["type"] == "quoted" or tweet["referenced_tweets"][0]["type"] == "retweeted":
                df.at[x, "Referenced_Tweet_Author"] = tweet["referenced_tweets"][0]["id"]
            else:
                df.at[x, "Referenced_Tweet_Author"] = "Keine"

        except:
            df.at[x, "Referenced_Tweet_Author"] = "Keine"
        
        x += 1
        
    return df



#Cleaning Funktion
nlp = spacy.load("de_core_news_lg")

def clean(text, RT_from, Reply_To):
    sw = ["amp"]

    if (RT_from == "Keine") and (Reply_To == "Keine"):        
        #For Bindestrich Words
        text = re.sub("-", "", text) 
        text = nlp(text)
        satz_clean = ""
        for token in text:
            if (token.is_stop != True) and (token.is_alpha == True):
                    if token.lemma_.lower() not in sw:
                        satz_clean = satz_clean + token.lemma_.lower() + " "
        return satz_clean.rstrip()

    else:
        return text




"""
Funktion zum Hinzufügen der Metadaten
Laden der aktiven und alten User-Datensätze
"""

#Laden der aktiven MdL/MdB/MdEP-Sets
files = os.listdir("Data/Datasets/Active")
df_aktive_user = pd.DataFrame()
for file in files:
    entpackt = pd.read_excel(f"Data/Datasets/Active/{file}")
    df_aktive_user = pd.concat([df_aktive_user, entpackt])
    

#Laden der alten MdL/MdB/MdEP-Sets
files = os.listdir("Data/Datasets/Old")
df_alte_user = pd.DataFrame()
for file in files:
    entpackt = pd.read_excel(f"Data/Datasets/Old/{file}")
    df_alte_user = pd.concat([df_alte_user, entpackt])

#Merge der alten u. neuen Usersets und Save in /Datensätze
df_overview_user = pd.concat([df_aktive_user, df_alte_user]).reset_index()
df_overview_user.drop_duplicates(inplace=True)
df_overview_user.reset_index(inplace=True)

df_aktive_user.to_excel("Data/Datasets/User_Active.xlsx")
df_overview_user.to_excel("Data/Datasets/User_Overview.xlsx")



#Erstelle Dict mit User-ID als Key und Infos als Values
dict_overview_user = {}
for counter in range(len(df_overview_user)):    
    if df_overview_user.at[counter, "Aktiv"] == "JA":
                               
                username = df_overview_user.at[counter, "Username"]
                partei = df_overview_user.at[counter, "Fraktion"]
                name = df_overview_user.at[counter, "Name"]
                alter = 2022 - int(df_overview_user.at[counter, "Geburtsjahr"])
                bundesland = df_overview_user.at[counter, "Land"]
                institution = df_overview_user.at[counter, "Institution"]

                dict_overview_user[df_overview_user.at[counter, "User-ID"][1:]] = [name, username, partei, alter, bundesland, institution]

def metadaten_return(user_id):
    return dict_overview_user[user_id]




if __name__ == "__main__":
    """
    -> Zusammenfassen der Tweets in einem Dataframe
    -> Bereinigung der Tweets unter "Text_Clean"
    -> Erfassen der Metadaten aus Twitter_Usernames_BT
    """

    #Import
    df = pd.DataFrame()
    
    #Laden aus JSON-Dateien
    files = os.listdir("Data/Tweets")
    
    for file in files:
        try:
            entpackt = öffnen(file)
            df = pd.concat([df, entpackt])
        except:
            print(file)
    
    len_vorher = len(df)
    df = df.drop_duplicates(subset=["Tweet-ID"]).reset_index()
    len_nachher = len(df)
    
    print("{} Tweets waren doppelt im Datensatz vorhanden und wurden entfernt.".format(len_vorher-len_nachher))
    
    #Bereinigung des Textes
    #df["Text_Clean"] = df.apply(lambda x: clean(x.Text, x.RT_from, x.Reply_To), axis=1)
    
    
    #Laden der Metadaten
    for x in range(len(df)):
        df.at[x, "Name"], df.at[x, "Username"], df.at[x, "Partei"], df.at[x, "Alter"], df.at[x, "Bundesland"], df.at[x, "Institution"] = metadaten_return(df.at[x, "Author-ID"])
    
    
    #Ändern des Datumsformats
    df['Datum'] = df['Datum'].astype('datetime64[ns]')
    
    for x in range(len(df)):
        datum = df.at[x, "Datum"]
        datum = datum.date()
        df.at[x, "Datum"] = datum    
    
    #Abspeichern
    df.to_excel("Data/Datasets/Tweets.xlsx")
    df.to_pickle("Data/Datasets/Tweets.pickle")