#clear the environment
globals().clear()
import pandas as pd
import numpy as np
from   scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
import time
import psycopg2
from tqdm import tqdm
pd.set_option('display.max_columns', 20)
pd.set_option('precision',2)


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

#Deal with dirctors' names in df
#df.at[] to add list into df
df.drop(columns=['titleId','language','isOriginalTitle','titleType'], inplace=True)
df.reset_index(inplace=True)
df.drop(columns='index', inplace = True)
df.replace(to_replace = r'\N', value = np.nan, inplace = True)
df_name.reset_index(inplace=True)
df_name.drop(columns = 'index', inplace = True)
df_name.replace(to_replace = r'\N', value = np.nan, inplace = True)


df = df[df['directors'].notna()]
len = df['directors'].apply(lambda row: len(row))

df_1 = df.copy()
df_1 = df_1[len<10]
df_2 = df.copy()[len>9]
df_2.reset_index(inplace=True)
df_2.drop(columns = 'index', inplace = True)

df_2_2 = pd.DataFrame()
for i in tqdm(df_2.index):
    temp = df_2.loc[i, 'directors'].split(',')
    for j in temp:
        temp2 = df_2.loc[i,:].copy()
        temp2['directors'] = j
        df_2_2 = df_2_2.append(temp2, ignore_index=True)
        #df_2_2.loc[-1, 'directors'] = j
df_sqlimport = df_2_2.append(df_1, ignore_index=True)
columnsTitles = ['title', 'region', 'tconst', 'startYear', 'runtimeMinutes', 'genres',
       'directors', 'averageRating', 'numVotes']

df_sqlimport = df_sqlimport.reindex(columns=columnsTitles)

#Save the temporary data as csv
def tempfilesave():
    df.to_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/Data_temp.csv')
    df_sqlimport.to_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/Data_sql.csv')
    df_name.to_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/name_temp.csv')
    #pd.DataFrame(unique_dirct).to_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/List_temp.csv')
tempfilesave()

#Easily to read next time
def tempfileread():
    df = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/Data_temp.csv',
                     index_col = 0,low_memory=False)
    df_name = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_name.csv',
                          index_col = 0)
    #Depend on specific which table to analyze
    df_flexible = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_exp.csv',
                          index_col = 0)
    summary = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_summary.csv',
                          index_col = 0)
    dflist = pd.read_csv('/Users/xintongli/PycharmProjects/Project/Film Data Analysis/List_temp')
    return df, df_name, df_flexible, summary, dflist

df      = tempfileread()[0].sort_index()
df_name = tempfileread()[1].sort_index()
year_a  = tempfileread()[2].sort_index()
summary = tempfileread()[3].sort_index()

df.columns = df.columns.str.lower()

#Overview of the all directors' data
summary['numberall'].describe()
df[['runtimeminutes', 'averagerating', 'numvotes']].describe()

print('Percentil of direct 1 film: ' +'{}'.format(stats.percentileofscore(summary['numberall'].to_list(),2)))
print('Percentil of direct 5 film: ' +'{}'.format(stats.percentileofscore(summary['numberall'].to_list(),5)))
print('The longest film is ' + '{}'.format(df['runtimeminutes'].max()) + ', number votes ' +
      '{}'.format(df['numvotes'][df['runtimeminutes'] == df['runtimeminutes'].max()].iloc[0]))
print('Percentage of lower than 100 votes ' +'{}'.format(stats.percentileofscore(df['numvotes'].to_list(),100)))

#Director's Career Length and Rating
print('Average career length: ' + '{}'.format(year_a['startyearlen'].mean()))
print('Average career length with multiple piece: ' +
      '{}'.format(year_a['startyearlen'][year_a['numberall']>1].mean()))

#Director's top rated film
fig, ax = plt.subplots()
ax.hist(year_a['toprate'],
        density = True, histtype = 'step', bins=20, label = 'Top Rate', color = 'Blue')
ax.hist(summary['averagerating'],
        density = True, alpha =0.2, bins=20, label = 'Average Rate', color = 'Blue')
ax.set_xlabel('Rating')
ax.set_ylabel('Density')
plt.tight_layout()
ax.legend(loc='upper left')
print(('Average top rating ' + '{}'.format(year_a['toprate'].mean())))
print(('Average rating ' + '{}'.format(summary['averagerating'].mean())))

print(('Mode of top rating ' + '{}'.format(year_a['toprate'].mode()[0])))
print(('Mode of rating ' + '{}'.format(summary['averagerating'].mode()[0])))





b = range(1,10)
a = list(range(1,10))
for i in b:
    a[i-1] = stats.percentileofscore(summary['numberall'].to_list(),i)
plt.plot(b,a)

summary['numberall'].hist()
df_total['averagerating'].hist()
plt.scatter(df_total['startyear'], df_total['age'])

#Which age does the director most often produce film
year_a
year_a['startyearlen'][year_a['count']>1].describe()
year_a['startyearlen'][year_a['count']>1].hist()

#Glimps of the data
sns.boxplot(df['numVotes'])
df['numVotes'].describe()
#The numVotes is heavily tailed, most movies only has less than 300 people to vote
np.corrcoef(df['numVotes'], df['averageRating'])
sns.scatterplot(x = 'numVotes', y = 'averageRating', data=df)

con = psycopg2.connect(
    #user name
)
#create a cursor
cur = conn.cursor()
#execute a query
cur.execute('SELECT title, directors FROM data')
row = cur.fetch

#commit the changes
con.commit
#close the cursor
cur.close()
#close the connection
con.close
