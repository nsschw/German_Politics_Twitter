import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv


def download(user_id, since_id = None):

    TIMELINE_URL_TEMPLATE = "https://api.twitter.com/2/users/{}/tweets"
    TOKEN = os.getenv('TOKEN')

    request_url = TIMELINE_URL_TEMPLATE.format(user_id) 

    #Formatieren der Start- und Endzeit damit nur aktuelle Tweets heruntergeladen werden. Weiterhin sind die heruntergeladenen Tweets mind. 48h online, sodass öff. Metriken auswertbar sind
    dtformat = '%Y-%m-%dT%H:%M:%SZ'
    time = datetime.utcnow()
    start_time = time - timedelta(days=31)
    end_time = time - timedelta(days=2)

    starttime, endtime = start_time.strftime(dtformat), end_time.strftime(dtformat)
     
    params = {"max_results": 100,"tweet.fields": 'author_id,created_at,lang,in_reply_to_user_id,public_metrics,source', "since_id": since_id, "end_time": endtime, "start_time": starttime, "expansions": "referenced_tweets.id.author_id"}

    response = requests.get(request_url, headers={"Authorization": f'BEARER {TOKEN}'}, params=params)
    tweets = response.json()


    größte_tweet_id = 0
    with open(f"Data/Tweets/tweets-{user_id}.jsonl", "a") as tweet_file:
        for tweet in tweets['data']:
            tweet_file.write(json.dumps(tweet) + "\n")
            größte_tweet_id = max((größte_tweet_id, int(tweet['id'])))

    return größte_tweet_id


def get_id(username):

    #Erhält Username als Input und gibt User-ID zurück

    TIMELINE_URL_TEMPLATE2 = "https://api.twitter.com/2/users/by/username/{}"
    TOKEN = os.getenv('TOKEN')

    request_url = TIMELINE_URL_TEMPLATE2.format(username)

    response = requests.get(request_url, headers={"Authorization": f'BEARER {TOKEN}'})
    profil = response.json()

    return profil["data"]["id"]


def main(df, path):    
    for x in range(len(df)):
        try:
            if df.at[x, "Aktiv"] == "JA":
                #Check, ob User-ID im Datensatz schon vorhanden ist und falls nicht downloaded diese
                user_id = df.at[x, "User-ID"]

                if type(user_id) != str:
                    username = df.at[x, "Username"]
                    user_id =  "#" + get_id(username)
                    df.at[x, "User-ID"] = user_id
                    time.sleep(3)


                #Check ob letzte Tweet-ID vorhanden und dann Download von allen neuen bzw. letzten 100 Tweets
                letzte_tweet_id = df.at[x, "Letzte Tweet-ID"]        


                if type(letzte_tweet_id) == str:
                    neue_letzte_id = download(user_id[1:], since_id = letzte_tweet_id[1:])  
                else:
                    neue_letzte_id = download(user_id[1:])


                #Letzte Tweet-ID einfügen
                df.at[x, "Letzte Tweet-ID"] = "#" + str(neue_letzte_id)

                df.at[x, "Fehler"] = ""

                #Speichern
                df.to_excel(path, index = False)

        except Exception as e:
            df.at[x, "Fehler"] = e
            df.to_excel(path)

        time.sleep(3)





if __name__ == "__main__":
    load_dotenv()

    alle = {"df_b": "MdL_Berlin_Liste.xlsx",
            "df_hh": "MdL_HH_Liste.xlsx",
            "df_ns": "MdL_NS_Liste.xlsx",
            "df_bb": "MdL_BB_Liste.xlsx",
            "df_s": "MdL_S_Liste.xlsx",
            "df_th": "MdL_TH_Liste.xlsx",    
            "df_bayern": "MdL_Bayern_Liste.xlsx",
            "df_BW": "MdL_BW_Liste.xlsx",
            "df_SA": "MdL_SA_Liste.xlsx",
            "df_MV": "MdL_MV_Liste.xlsx",
            "df_EP": "MdEP_Liste.xlsx",
            "df_SL": "MdL_SL_Liste.xlsx",
            "df_nrw": "MdL_NRW_Liste.xlsx",
            "df_sh": "MdL_SH_Liste.xlsx",
            "df_rlp": "MdL_RLP_Liste.xlsx",        
            "df_bund": "MdB_Liste.xlsx",
            "df_bremen": "MdL_Bremen_Liste.xlsx",
            "df_hessen": "MdL_Hessen_Liste.xlsx"}

    for item in alle:
        path = f"Data/Datasets/{alle[item]}"    
        df = pd.read_excel(path)
        main(df, path)
