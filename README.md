# twitter_extractor
practice with twitter api through Python with tweepy lib.

ETL exercice with twitter API, python, tweepy, pandas and mysql

objetives:
  -Extract recent tweets for a keyword. 
    fields = id, created_at, text, source, in_reply_to_user_id, author_id, retweets, likes, language
  -Generate a csv file with data extracted.
  -Generate a dataframe with pandas of the csv file
  -Clean the dataframe
  -Export the dataframe to a MySQL database


The API keys and database credentials are saved in yaml file
config.yaml structure:

twitter:
  keys:
    api_key: 'your_key'
    api_secrect: 'your_secrect'
    token: 'your_token'
    token_secrect: 'your_secrect_token'
database:
  host: 'your_database_host'
  port: 'your_database_port'
  user: 'your_database_user'
  pass: 'your_database_password'
  db: 'your_database_name'
