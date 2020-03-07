#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
from config import db_password
import time


# In[2]:


pip install psycopg2


# In[3]:


file_dir = 'C:/Users/judyc/OneDrive/Desktop/Class/'
wiki_file = 'C:/Users/judyc/OneDrive/Desktop/Class/wikipedia.movies.json'
kaggle_file = 'C:/Users/judyc/OneDrive/Desktop/Class/movies_metadata.csv'
ratings_file = 'C:/Users/judyc/OneDrive/Desktop/Class/ratings.csv'


# In[4]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles
    #merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie [new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
     
    
    return movie


# In[5]:


def parse_dollars(s):
    #if s is not a string, return NaN
    if type(s)!= str:
        return np.nan
    
    #if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s,flags=re.IGNORECASE):
        
        #remove dollar sign and "million"
        s=re.sub('\$|\s|[a-zA-Z]','',s)
        
        #convert to float and muliply by a million
        value = float(s) *10**6
        
        #return value
        return value
        
        
    #if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s,flags=re.IGNORECASE):
        
        #remove dollar sign and "billion"
        s=re.sub('\$|\s|[a-zA-Z]','',s)
        
        #convert to float and multiply by a billion
        value = float(s) *10**9
        
        #return value
        return value
    
    #if input of the form $###,###,###
    elif re.match(r'\$\d{1,3}(,\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        
        #remove dollar sign and commas
        s=re.sub('\$|,','', s)  
        
        #convert to float
        value= float(s)
        
        #return value
        
        return value
    
    #otherwise, return NaN
    
    else:
        
        return np.nan
        
    


# In[6]:


def is_not_a_string(x):
    return type(x) != str


# In[7]:


#here is where I wanted to create a function
def extract_transform_load(wiki_file, kaggle_file, ratings_file):
    with open(wiki_file, mode = 'r') as file:
        wiki_movies_raw = json.load(file)
    
    kaggle_metadata = pd.read_csv(kaggle_file, low_memory=False)
    ratings = pd.read_csv(ratings_file)
    wiki_movies = [movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie and 'No. of episodes' not in movie]                  
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    # Assuming wikipedia data still contains IMDb id
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    except Exception as e:
        print(e)
#trim down to less than 90% null values
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df)*0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    box_office = wiki_movies_df['Box office'].dropna()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) ==list else x)
    form_one = r'\$\d+\.?\d*\s*[mb]illion'
    form_two = r'\$\d{1,3}(?:,\d{3})+'
    matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)
    box_office[~matches_form_one & ~matches_form_two]
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    box_office.str.extract(f'({form_one}|{form_two})')
    return box_office, wiki_movies_df, kaggle_metadata, ratings #wiki_movies


# In[8]:


box_office,wiki_movies_df, kaggle_metadata,ratings = extract_transform_load(wiki_file, kaggle_file, ratings_file)
wiki_movies_df.head()


# In[9]:


form_one =r'\$\d+\.?\d*\s*[mb]illion'
form_two = r'\$\d{1,3}(,\d{3})+'


# In[10]:


#create boolean series to see which values are described by either form one or form two
matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)


# In[11]:


box_office[~matches_form_one & ~matches_form_two]


# In[12]:


box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)


# In[13]:


box_office.str.extract(f'({form_one}|{form_two})')


# In[14]:


#creates the new box_office column
wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


# In[15]:


wiki_movies_df.drop('Box office', axis=1, inplace=True)


# In[16]:


#Now let's create a budget variable
budget = wiki_movies_df['Budget'].dropna()


# In[17]:


#convert lists to strings using lambda function
budget = budget.map(lambda x: ' '.join (x) if type(x) == list else x)


# In[18]:


budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)


# In[19]:


#might be able to delete the second instance of the regex
form_one =r'\$\d+\.?\d*\s*[mb]illion'


# In[20]:


#might be able to delect second instance
form_two = r'\$\d{1,3}(,\d{3})+'


# In[21]:


matches_form_one =budget.str.contains(form_one, flags=re.IGNORECASE)
matches_form_two =budget.str.contains(form_two, flags=re.IGNORECASE)
budget[~matches_form_one & ~matches_form_two]


# In[22]:


budget = budget.str.replace(r'\[\d+\]\s*', ' ')
budget[~matches_form_one & ~matches_form_two]


# In[23]:


wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


# In[24]:


wiki_movies_df.drop('Budget', axis=1, inplace=True)


# In[25]:


release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type == list else x )


# In[26]:


date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'


# In[27]:


release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)


# In[28]:


wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0],infer_datetime_format=True)


# In[29]:


wiki_movies_df.drop('Release date', axis=1, inplace=True)


# In[30]:


#parse running time
running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)


# In[31]:


running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE).sum()


# In[32]:


running_time[running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE) != True]


# In[33]:


running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE).sum()


# In[34]:


running_time[running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE) != True]


# In[35]:


running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')


# In[36]:


running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)


# In[37]:


wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)


# In[38]:


wiki_movies_df.drop('Running time', axis=1, inplace=True)


# In[39]:


box_office,wiki_movies_df, kaggle_metadata, ratings = extract_transform_load(wiki_file, kaggle_file, ratings_file)
kaggle_metadata.head()


# In[40]:


kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult', axis='columns')


# In[41]:


kaggle_metadata['video'] == 'True'


# In[42]:


kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'


# In[43]:


kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')


# In[44]:


kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])


# In[45]:


pd.to_datetime(ratings['timestamp'], unit='s')


# In[46]:


ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')


# In[47]:


movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes= ['_wiki','_kaggle'])


# In[48]:


movies_df['Language'].apply(lambda x: tuple(x) if type(x)== list else x).value_counts(dropna=False)


# In[49]:


movies_df['original_language'].value_counts(dropna=False)


# In[50]:


#flat out drop columns that are not workable
movies_df.drop(columns=['title_wiki','Language','Production company(s)'], inplace=True)


# In[51]:


#create a function to fill in missing data for a column pair and then drop the redundant column
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1 )
    df.drop(columns=wiki_column, inplace=True)


# In[52]:


#now run the function for the three column pairs we decided to fill in with zeroes
#fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
#fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
#fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
movies_df


# In[53]:


for col in movies_df.columns:
    lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
    value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
    num_values = len(value_counts)
    if num_values == 1:
        print(col)


# In[54]:


movies_df.drop(columns=['video'])


# In[55]:


movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','revenue','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]


# In[56]:


movies_df.rename({'id':'kaggle_id',
                 'title_kaggle':'title',
                 'url':'wikipedia_url',
                 'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)


# ## Transform and merge rating data

# In[57]:


rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()


# In[58]:


rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1)


# In[59]:


rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1)                 .pivot(index='movieId',columns='rating', values='count')


# In[60]:


#rename the columns in the ratings so they are easier to understand
rating_counts.columns =['rating_' + str(col) for col in rating_counts.columns]


# In[61]:


#create a new DataFrame with a left join merge of the movie and ratings data
movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')


# In[62]:


movies_with_ratings_df = movies_with_ratings_df[rating_counts.columns].fillna(0)


# In[63]:


db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"


# In[64]:


engine = create_engine(db_string)


# In[65]:


movies_df.to_sql(name = 'movies', con=engine, if_exists='replace')


# In[68]:


#create a variable for the number of rows imported and initialize to zero
rows_imported = 0
#get the start_time from time.time()
start_time = time.time()
for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
    
    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    data.to_sql(name='ratings2', con=engine, if_exists='append')
    rows_imported += len(data)
    # add elapsed time to final print out
    print('Done. {time.time() - start_time} total seconds elapsed')


# In[ ]:




