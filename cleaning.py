import pandas as pd

IMDB_csv = pd.read_csv("IMBD1.csv", doublequote=True)
df = pd.DataFrame(IMDB_csv)

df2= pd.DataFrame(IMDB_csv)

df_genre2 = df["genre"]
df_genre2 = df_genre2.str.strip("[")
df_genre2 = df_genre2.str.rstrip(",")
df_genre2 = df_genre2.str.strip("]")
df_genre2 = df_genre2.str.split(',', n=2).str.get(1)
df_genre2= df_genre2.str.replace(" ","")
df2["genre"]=df_genre2


df_stars= df['stars']
df_stars2 = df_stars.str.strip("[")
df_stars2 = df_stars2.str.rstrip(",")
df_stars2 = df_stars2.str.strip("]")
df_stars2 = df_stars2.str.split(',', n=1).str.get(0)
df_stars2 = df_stars2.str.replace('\'',"")

df2["stars"]=df_stars2

df_rt= df["runtime"]
df_rt= df_rt.str.replace(" min","")

df_votes=df["votes"].str.replace(",","")

df2["runtime"]=df_rt

df_dir = df['director'].str.replace("[","")
df_dir = df_dir.str.replace("]","")
df_dir = df_dir.str.replace("'","")

df2["director"] = df_dir

df_movie= df["movie"].str.replace("\'","")
df_movie= df["movie"].str.replace("\'","")



df2["movie"] = df_movie

df2 = df2[['movie','genre','runtime','certificate','rating','stars','votes','director']].replace("\'\'","")
df2.to_csv('cleaned_csv25.csv',index = False)