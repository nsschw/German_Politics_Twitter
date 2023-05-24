import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.manifold import TSNE
import umap



def reduce_and_plot_embeddings(df, method = "umap", filter_df=True, average = True):

    if filter_df:
        df = df[df["RT_from"] == "Keine"]
        df = df[df["Reply_To"] == "Keine"]
    df = df.reset_index(drop=True)


    #average embeddings of tweets per user but save the party per user
    if average:
        df = df.groupby('Username').agg({"Partei": "first", 'embedding': np.mean}).reset_index()
    
    if method == "tsne":
        reducer = TSNE(n_components=2, random_state=0)
    elif method == "umap":
        reducer = umap.UMAP(random_state=0)

    embeddings = np.array(df["embedding"].tolist())
    reduced_embeddings = reducer.fit_transform(embeddings)

    df["x"] = reduced_embeddings[:,0]
    df["y"] = reduced_embeddings[:,1]

    fig = px.scatter(df, x="x", y="y", color="Partei")
    fig.show()





def calculate_cos_distance_between_embeddings(df, min_user_per_party=10, min_tweets_per_user=30):

    #filter out users with less than min_tweets_per_user tweets
    df = df.groupby('Username').filter(lambda x: len(x) >= min_tweets_per_user)
    df = df.reset_index(drop=True)    

    #filter out parties with less than min_user_per_party users
    df_count_user = df.groupby('Partei').agg({"Username": "nunique"}).reset_index()
    df_count_user = df_count_user[df_count_user["Username"] >= min_user_per_party]
    df = df[df["Partei"].isin(df_count_user["Partei"].tolist())]
    df = df.reset_index(drop=True)
    
    #filter out retweets and replies
    df = df[df["RT_from"] == "Keine"]
    df = df[df["Reply_To"] == "Keine"]
    df = df.reset_index(drop=True)

    df = df.groupby('Username').agg({"Partei": "first", 'embedding': np.mean}).reset_index()

    for party in df["Partei"].unique():

        #calculate cosinus distance between all embeddings of a party and average them
        party_df = df[df["Partei"] == party]
        embeddings = np.array(party_df["embedding"].tolist())
        distances = np.zeros((len(embeddings), len(embeddings)))

        for i in range(len(embeddings)):
            for j in range(len(embeddings)):                
                distances[i,j] = np.dot(embeddings[i], embeddings[j])/(np.linalg.norm(embeddings[i])*np.linalg.norm(embeddings[j]))

        print(party, np.mean(distances))
    

    #calculate cosinus distance between all embeddings of two different parties and average them and plot heatmap
    df_party_distances = pd.DataFrame()
    for party1 in df["Partei"].unique():
        for party2 in df["Partei"].unique():
            if party1 != party2:
                party1_df = df[df["Partei"] == party1]
                party2_df = df[df["Partei"] == party2]
                embeddings1 = np.array(party1_df["embedding"].tolist())
                embeddings2 = np.array(party2_df["embedding"].tolist())
                distances = np.zeros((len(embeddings1), len(embeddings2)))

                for i in range(len(embeddings1)):
                    for j in range(len(embeddings2)):                
                        distances[i,j] = np.dot(embeddings1[i], embeddings2[j])/(np.linalg.norm(embeddings1[i])*np.linalg.norm(embeddings2[j]))

                df_party_distances = pd.concat([df_party_distances, pd.DataFrame({"partei": party1, "partei2": party2, "wert": np.mean(distances)}, index=[0])], axis=0)

            else:
                df_party_distances = pd.concat([df_party_distances, pd.DataFrame({"partei": party1, "partei2": party2, "wert":np.NaN}, index=[0])], axis=0)


        
    print(df_party_distances)
    fig = go.Figure(data=go.Heatmap(
                    z=df_party_distances.wert,
                    x=df_party_distances.partei,
                    y=df_party_distances.partei2,
                    colorscale=["red", "white", "blue"])
                    )
    fig.show()
    
    



if __name__ == '__main__':

    df = pd.read_pickle("Data/Datasets/Tweets.pickle")
    calculate_cos_distance_between_embeddings(df)