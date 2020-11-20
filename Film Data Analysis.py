import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import time
#import from dataset
pd.set_option('display.max_columns', 20)
def import_raw_file():
    df_name = pd.read_csv('/Volumes/未命名/学习/imdb Dataset/name.basics.tsv', sep = '\t',
                        usecols = ['nconst', 'primaryName','birthYear','deathYear'],
                          low_memory=False)
    df_akas = pd.read_csv('/Volumes/未命名/学习/imdb Dataset/title.akas.tsv', sep = '\t',
                      usecols=['titleId','title','region','language','isOriginalTitle'],
                      low_memory=False)
    df_basics = pd.read_csv('/Volumes/未命名/学习/imdb Dataset/title.basics.tsv', sep = '\t',
                           usecols=['tconst','titleType','startYear','runtimeMinutes','genres'],
                            low_memory=False)
    df_crew = pd.read_csv('/Volumes/未命名/学习/imdb Dataset/title.crew.tsv', sep = '\t',
                           usecols=['tconst','directors'],
                            low_memory=False)
    df_rating = pd.read_csv('/Volumes/未命名/学习/imdb Dataset/title.ratings.tsv', sep = '\t',
                            low_memory=False)
    return df_name, df_akas, df_basics, df_crew, df_rating
df_name, df_akas, df_basics, df_crew, df_rating = import_raw_file()

#Data cleaning
#filter df_name, leave with director's info
unique_dirct =df_crew['directors'].unique()
unique_dirct = unique_dirct.tolist()
temp = []
for i in unique_dirct:
    if len(i) < 10:
        temp.append(i)
    if len(i) > 9:
        for j in i.split(','):
            temp.append(j)
unique_dirct = list(set(temp))
df_name = df_name[df_name['nconst'].isin(unique_dirct)]
#Deal with Duplication in the df_akas
df_basics = df_basics[df_basics['titleType']=='movie']
duplicate = df_akas[df_akas['titleId'].duplicated(keep = False)]
duplicate['isOriginalTitle'].value_counts()
temp = duplicate[duplicate['isOriginalTitle'].isin(['0',r'\N'])]
df_akas = pd.concat([df_akas, temp]).drop_duplicates(keep=False)
#Merge movie data together
df = df_akas.merge(df_basics, how = 'right', left_on = 'titleId', right_on = 'tconst')
df = df.merge(df_crew, how = 'left', left_on='tconst', right_on = 'tconst')
df = df.merge(df_rating, how = 'left', left_on='tconst', right_on = 'tconst')
#Drop no-rating movies
df.dropna(subset = ['averageRating'],inplace = True)

#Save the temporary data as csv
def tempfilesave():
    df.to_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/Data_temp')
    df_name.to_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/name_temp')
    pd.DataFrame(unique_dirct).to_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/List_temp')
tempfilesave()

#Easily to read next time
def tempfileread():
    df = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/Data_temp',
                     index_col = 0,low_memory=False)
    df_name = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/name_temp',
                          index_col = 0)
    dflist = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/List_temp')
    return df, df_name
df, df_name = tempfileread()


#Deal with dirctors' names in df
#df.at[] to add list into df
df.drop(columns=['tconst','isOriginalTitle','titleType'], inplace=True)
df.reset_index(inplace=True)
df_name.reset_index(inplace=True)
df_name.drop(columns = 'index', inplace = True)

c = df.copy()
b = df_name.copy()

###Able to run, but extremely slow for large data, need different method
#Try to deal with this process by using the postgresql database
'''start = time.time()
for i in range(0, 100):
    a = c[c['directors'].str.contains(b.loc[i, 'nconst'])]
    b.loc[i, 'numFilms'] = len(a)
    if len(a) > 1:
        temp = ''
        for j in a['titleId']:
            temp  += ',' + j
        temp = temp[1:]
        b.loc[i, 'film'] = temp
    elif len(a) == 1:
        b.loc[i, 'film'] = a['titleId'].values[0]
    else:
        b.loc[i, 'film'] = np.nan
end = time.time()
print(end - start)'''

#Glimps of the data
sns.boxplot(df['numVotes'])
df['numVotes'].describe()
#The numVotes is heavily tailed, most movies only has less than 300 people to vote
np.corrcoef(df['numVotes'], df['averageRating'])
sns.scatterplot(x = 'numVotes', y = 'averageRating', data=df)