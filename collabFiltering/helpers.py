import pandas as pd

def process_raw_file(input_filename,output_filename):
    with open(input_filename, 'r') as raw:
        with open(output_filename,'w') as output:
            output.write('user_id,ratings,date,movie_id\n')
            movie_id = 'init'
            for line in raw :
                if ':' in line : movie_id = line[:-2]
                else :
                    to_write = line[:-1] + ',' + movie_id + '\n'
                    output.write(to_write)

def compress_data(df:pd.DataFrame):
    '''
    Reduce amount of movies
    Finds the optimum between :
    - Deletting as many movies as possible
    - Keeping as many views as possible
    Ranks movies by importance in terms of average of daily views
    '''

    # We normalise the views by the time the movie has been on the platform (in days)
    # That will be our movie importance value
    movie_views = df.groupby('movie_id').agg({'user_id':'count','date':'min'}).rename(columns={'user_id':'view_count','date':'release_date'}).reset_index()
    movie_views['time_available'] = df['date'].max() - movie_views['release_date']
    movie_views['time_available'] = movie_views['time_available'].apply(lambda x : x.days)
    movie_views['view_count_norm'] = movie_views['view_count'] / movie_views['time_available'].astype('int')

    # Starting with the movie that's the most successful in terms of normalised view, we count how many views remain
    # if we were to drop all the movies below (cumsum) and later compute the % in terms of all the views
    movie_views = movie_views.sort_values('view_count_norm',ascending=False)
    movie_views = movie_views.reset_index().drop(columns='index').reset_index()
    movie_views['views_left'] = movie_views['view_count'].cumsum()

    # Similarily we count how many movies are left in % (we wan't to compare how many views left with how many movies left)
    movie_views['movie_left_perc'] = (movie_views['index'] + 1) / (movie_views['index'].max() + 1)
    movie_views['views_left_perc'] = movie_views['views_left'] / movie_views['view_count'].sum()
    movie_views = movie_views.sort_values('view_count_norm')

    # Find the difference between views left (%) and movies left (%)
    movie_views['diff_views_movie'] = movie_views['views_left_perc'] - movie_views['movie_left_perc']
    # Find the maximum of this difference and remove everything up to it
    movies_to_drop = movie_views.iloc[:movie_views['diff_views_movie'].argmax()]['movie_id']
    print('Statistics')
    print(movie_views.iloc[movie_views['diff_views_movie'].argmax()][['movie_left_perc','views_left_perc']])
    del movie_views
    return df.set_index('movie_id').drop(movies_to_drop).reset_index().copy()
