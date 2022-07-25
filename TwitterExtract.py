import tweepy
import regex as re
import string
import pandas as pd
import numpy as np
import textstat
from pysentimiento import create_analyzer
import time
import warnings
import cx_Oracle
import ConnectionInfo

warnings.filterwarnings('ignore')
conn = cx_Oracle.connect(ConnectionInfo.usrnm, ConnectionInfo.psswd, ConnectionInfo.dsn)
print('Connection successful')
conn.autocommit = True

APIkey = 'apikey'
APIsecret = 'apisecret'

# Authenticate to Twitter
auth = tweepy.OAuthHandler(consumer_key=APIkey, consumer_secret=APIsecret)

api = tweepy.API(auth, wait_on_rate_limit=True)


def scrape(text_query, count=10):
    global conn

    search = tweepy.Cursor(api.search_tweets, q=text_query, tweet_mode='extended').items(count)
    for s in search:

        vOf = 0
        if s.user.verified:
            vOf = 1
        regex_zona = (re.search('\+{1}.+', str(s.created_at)).group(0))
        creado = str(s.created_at).replace(regex_zona, '')

        c = conn.cursor()
        try:
            c.execute("""INSERT INTO WKSP_HACKATHON.USERS VALUES (:s, :s2, :s3, :s4, :s5, :s6, :s7)""",
                      [s.user.id, s.user.name, s.user.location, s.user.followers_count, s.user.friends_count,
                       s.user.statuses_count, int(vOf)])
            print('succesful user insert')
        except (cx_Oracle.IntegrityError, cx_Oracle.DatabaseError):
            print('unsuccessful user insert')

        if s.place is not None:
            try:
                c.execute("""INSERT INTO WKSP_HACKATHON.PLACE VALUES(:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, 
                :v11)""", [s.place.id, s.place.full_name, s.place.country, s.place.bounding_box.coordinates[0][0][0],
                           s.place.bounding_box.coordinates[0][0][1], s.place.bounding_box.coordinates[0][1][0],
                           s.place.bounding_box.coordinates[0][1][1], s.place.bounding_box.coordinates[0][2][0],
                           s.place.bounding_box.coordinates[0][2][1], s.place.bounding_box.coordinates[0][3][0],
                           s.place.bounding_box.coordinates[0][3][1]])
                print('sucessful place insert')
            except cx_Oracle.IntegrityError:
                print('unsucessful place insert')

            tweet_info = {'tweet_id': s.id,
                          'user_id': s.user.id,
                          'place_id': s.place.id,
                          'created_at': creado,
                          'tweet_text': s.full_text,
                          'retweet_count': s.retweet_count,
                          'like_count': s.favorite_count}

            try:
                c.execute("""INSERT INTO WKSP_HACKATHON.TWEET VALUES(:v1, :v2, :v3, :v4, :v5, :v6, :v7)""",
                          [s.id, s.user.id, s.place.id, creado, s.full_text, s.retweet_count, s.favorite_count])
                print('successful tweet insert')
            except (cx_Oracle.IntegrityError, cx_Oracle.DatabaseError):
                print('unsuccessful tweet insert')

        else:
            tweet_info = {'tweet_id': s.id,
                          'user_id': s.user.id,
                          'place_id': 'NULL',
                          'created_at': creado,
                          'tweet_text': s.full_text,
                          'retweet_count': s.retweet_count,
                          'like_count': s.favorite_count}


            try:
                c.execute("""INSERT INTO WKSP_HACKATHON.TWEET VALUES(:v1, :v2, :v3, :v4, :v5, :v6, :v7)""",
                          [s.id, s.user.id, '', s.created_at, s.full_text, s.retweet_count, s.favorite_count])
                print('successful tweet insert')
            except (cx_Oracle.IntegrityError, cx_Oracle.DatabaseError):
                print('unsuccesful tweet insert')

        # json tweet to df
        tweets_df = pd.DataFrame.from_dict([tweet_info])

        # Creacion nueva tabla
        df_processing = tweets_df.drop(tweets_df.columns[[2, 3, 5, 6]], axis=1)
        # Quitar IDS
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "849105447990682000"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "1319738898226970000"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "764657184"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "107342719"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "158118026"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "1080608431529180000"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "1480924115385140000"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "1898396389"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "418568638"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "619577214"].index)
        df_processing = df_processing.drop(df_processing[df_processing['user_id'] == "403486458"].index)
        df_processing = df_processing.drop_duplicates(subset=['tweet_text'], keep='first')
        del (df_processing['user_id'])

        # Limpieza y creacion de columnas
        def proceso(text):
            # Creacion columna limpia
            df_processing['CleanTweet'] = df_processing['tweet_text']
            # exclude emails
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'\S*@\S*\s?', '')
            # Exclude mentions
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'@\S*\s?', '')
            # exclude urls
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace('http\S+|www.\S+', '', case=False)
            # Exclude symbols or punctuations
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'\?|\\|\!|\"|\#|\$|\%|\&|\'|\[|\^|\||\,|\¿|\¡|\_|\=|\>|\[|\^|\`|\{|\}|\~|\[|\]|\*|\+|\@|\/|\-|\:|\?|\¡|\¿||\.|\\|\“|\”|\(|\)|\;|\’|\;|\`|\´|\-|\·|\<|\º|\ª','')
            # Exclude special case symbol '\nd'
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'\nd', '')
            # Exclude special case symbol '\n'
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'\n', '')
            # Exclude cases of hashtags
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'\B(\#[a-zA-Z]+\b)(?!;)', '')
            # Lower Case process
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.lower()
            # Eliminate words length <= 2
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'\b\w{1,2}\b', '')
            # Exclude context words
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'izcalli', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'izcall', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'cuautitln', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'uautitln', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'edomex', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'edomexico', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'cuautitlan', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'cuautitlán', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'méxico', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'mxico', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'cuautitlanizcalli', '')
            df_processing['CleanTweet'] = df_processing['CleanTweet'].str.replace(r'cuautitlánizcalli', '')

            # new
            # CREACION DE COLUMNAS NUEVAS
            # Lenght
            df_processing['LengthReview'] = df_processing['tweet_text'].map(lambda x: len(x))
            # Punctuations
            count = lambda l1, l2: sum([1 for x in l1 if x in l2])
            df_processing['NumPunctuations'] = df_processing['tweet_text'].apply(lambda s: count(s, string.punctuation))
            # Hasthtag
            df_processing['NumHashtags'] = df_processing['tweet_text'].str.count('#')
            # Num sentences
            # Works but only on punctuation predefined
            df_processing['NumSentences'] = df_processing['tweet_text'].apply(textstat.sentence_count)
            # Num words
            df_processing['Numwords'] = df_processing['tweet_text'].apply(lambda x: len(str(x).split(' ')))
            # Mentions
            df_processing['NumAtSigns'] = df_processing['tweet_text'].str.count('@')
            # Number of tweet
            df_processing['Number_of_Tweet'] = df_processing['tweet_text'].map(tweets_df.tweet_text.value_counts())
            # Mean of word length used in each tweet
            df_processing['MeanWordLength'] = df_processing['tweet_text'].apply(
                lambda x: np.mean(len(str(x).split(' '))))
            # Characters
            df_processing['NumCharacters'] = df_processing['tweet_text'].str.len()

            # hastags usados
            def find_tags(row_string):
                # use a list comprehension to find list items that start with #
                tags = [x for x in row_string if x.startswith('#')]
                return tags

            # Creacion columna limpia
            df_processing['CleanTweettags'] = df_processing['tweet_text']
            # exclude urls
            df_processing['CleanTweettags'] = df_processing['CleanTweettags'].str.replace('http\S+|www.\S+', '',
                                                                                          case=False)
            df_processing['split'] = df_processing['CleanTweettags'].str.split(' ')
            df_processing['tags'] = df_processing['split'].apply(lambda row: find_tags(row))
            # replace # as requested in OP, replace for new lines and \ as needed.
            df_processing['tags'] = df_processing['tags'].apply(
                lambda x: str(x).replace('#', '').replace('\\n', ',').replace('\\', '').replace("'", ""))

            # Nueva columna clean tag
            # exclude emails
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'\S*@\S*\s?', '')
            # Exclude mentions
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'@\S*\s?', '')
            # exclude urls
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'https\S+|www.\S+', '', case=False)
            # Exclude symbols or punctuations
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'\?|\\|\!|\"|\#|\$|\%|\&|\'|\[|\^|\||\,|\¿|\¡|\_|\=|\>|\[|\^|\`|\{|\}|\~|\[|\]|\*|\+|\@|\/|\-|\:|\?|\¡|\¿||\.|\\|\“|\”|\(|\)|\;|\’|\;|\`|\´|\-|\·|\<|\º|\ª','')
            # Exclude special case symbol '\nd'
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'\nd', '')
            # Exclude special case symbol '\n'
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'\n', '')
            # Exclude cases of hashtags
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'\B(\#[a-zA-Z]+\b)(?!;)', '')
            # Eliminate words length <= 2
            df_processing['clean_tag'] = df_processing['tags'].str.replace(r'\b\w{1,2}\b', '')

            # Eliminar columnas innecesarias
            del (df_processing['CleanTweettags'])
            return text

        proceso(df_processing)

        # Modelos
        analyzer = create_analyzer(task="sentiment", lang="es")
        hate_speech_analyzer = create_analyzer(task="hate_speech", lang="es")
        emotion_analyzer = create_analyzer(task="emotion", lang="es")

        def processSentiment(row):
            res = analyzer.predict(row["CleanTweet"])
            return pd.Series({'sentiment': res.output, **res.probas})

        def processHate(row):
            res = hate_speech_analyzer.predict(row["CleanTweet"])
            return pd.Series({'tipo': res.output, **res.probas})

        def processemotion(row):
            res = emotion_analyzer.predict(row["CleanTweet"])
            return pd.Series({'Emocion': res.output, **res.probas})

        df_processing = df_processing.join(df_processing.apply(processSentiment, axis=1))
        df_processing = df_processing.join(df_processing.apply(processHate, axis=1))
        df_processing = df_processing.join(df_processing.apply(processemotion, axis=1))

        df = df_processing

        def limpieza(text):
            df['tipo'] = df['tipo'].str.replace(r'\W', ' ')
            df['tags'] = df['tags'].str.replace(r'\W', ' ')
            df['clean_tag'] = df['clean_tag'].str.replace(r'\W', ' ')
            df['split'] = df['tags'].str.replace(r'\W', ' ')
            return text
        limpieza(df)

        tweet_info1 = {}
        for index, row in df.iterrows():
            tweet_info1['tweet_id_pk'] = row['tweet_id']
            tweet_info1['tweet_id'] = row['tweet_id']
            tweet_info1['tweet_text'] = row['tweet_text']
            tweet_info1['CleanTweet'] = row['CleanTweet']
            tweet_info1['LengthReview'] = row['LengthReview']
            tweet_info1['NumPunctuations'] = row['NumPunctuations']
            tweet_info1['NumHashtags'] = row['NumHashtags']
            tweet_info1['NumSentences'] = row['NumSentences']
            tweet_info1['Numwords'] = row['Numwords']
            tweet_info1['NumAtSigns'] = row['NumAtSigns']
            tweet_info1['Number_of_Tweet'] = row['Number_of_Tweet']
            tweet_info1['MeanWordLength'] = row['MeanWordLength']
            tweet_info1['NumCharacters'] = row['NumCharacters']
            tweet_info1['split'] = row['split']
            tweet_info1['tags'] = row['tags']
            tweet_info1['clean_tag'] = row['clean_tag']
            tweet_info1['sentiment'] = row['sentiment']
            tweet_info1['NEG'] = row['NEG']
            tweet_info1['NEU'] = row['NEU']
            tweet_info1['POS'] = row['POS']
            tweet_info1['tipo'] = row['tipo']
            tweet_info1['hateful'] = row['hateful']
            tweet_info1['targeted'] = row['targeted']
            tweet_info1['aggressive'] = row['aggressive']
            tweet_info1['Emocion'] = row['Emocion']
            tweet_info1['others'] = row['others']
            tweet_info1['joy'] = row['joy']
            tweet_info1['sadness'] = row['sadness']
            tweet_info1['anger'] = row['anger']
            tweet_info1['surprise'] = row['surprise']
            tweet_info1['disgust'] = row['disgust']
            tweet_info1['fear'] = row['fear']

        print(tweet_info1)

        try:
            c.execute("""INSERT INTO WKSP_HACKATHON.MODEL (tweet_id_pk,tweet_id,tweet_text,CleanTweet,LengthReview,
            NumPunctuations,NumHashtags,NumSentences,Numwords,NumAtSigns,Number_of_Tweet,MeanWordLength,NumCharacters,
            split,tags,clean_tag,sentiment,NEG,NEU,POS,tipo,hateful,targeted,aggressive,Emocion,others,joy,sadness,
            anger,surprise,disgust,fear) VALUES(:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, :v11, :v12,
            :v13, :v14, :v15, :v16, :v17, :v18, :v19, :v20, :v21, :v22, :v23, :v24, :v25, :v26, :v27, :v28,
            :v29, :v30, :v31, :v32)""",
                      [tweet_info1.get('tweet_id_pk'), tweet_info1.get('tweet_id'), tweet_info1.get('tweet_text'),
                       tweet_info1.get('CleanTweet'), tweet_info1.get('LengthReview'),
                       tweet_info1.get('NumPunctuations'), tweet_info1.get('NumHashtags'),
                       tweet_info1.get('NumSentences'), tweet_info1.get('Numwords'), tweet_info1.get('NumAtSigns'),
                       tweet_info1.get('Number_of_Tweet'), tweet_info1.get('MeanWordLength'),
                       tweet_info1.get('NumCharacters'), tweet_info1.get('split'), tweet_info1.get('tags'),
                       tweet_info1.get('clean_tag'), tweet_info1.get('sentiment'), tweet_info1.get('NEG'),
                       tweet_info1.get('NEU'), tweet_info1.get('POS'), str(tweet_info1.get('tipo')),
                       tweet_info1.get('hateful'), tweet_info1.get('targeted'), tweet_info1.get('aggressive'),
                       tweet_info1.get('Emocion'), tweet_info1.get('others'), tweet_info1.get('joy'),
                       tweet_info1.get('sadness'), tweet_info1.get('anger'), tweet_info1.get('surprise'),
                       tweet_info1.get('disgust'), tweet_info1.get('fear')])
            print('model insert successful')
        except (cx_Oracle.IntegrityError, cx_Oracle.DatabaseError):
            print('model insert unsucessful')

        # limpiar tweet_info
        tweet_info1 = {}



while True:
    scrape('Izcalli', 999)
    scrape('Cuautitlan', 999)
    scrape('CuautitlanIzcalli', 999)
    scrape('izcaheroes', 999)
    scrape('KarlaFiesco', 999)
    scrape('IzcalliDenuncia',  999)
    scrape('SPublicaIzcalli', 999)

    print('is sleeping...')
    time.sleep(1)










