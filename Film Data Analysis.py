import pandas as pd
import numpy as np
df_name=pd.read_csv('/Volumes/未命名/学习/imdb Dataset/name.basics.tsv', sep = '\t')
df_name = df_name.drop(columns=['primaryProfession', 'knownForTitles'])
df2=pd.read_csv(, sep = '\t')
