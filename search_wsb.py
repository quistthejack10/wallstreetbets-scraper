from psaw import PushshiftAPI
import config
import datetime
import psycopg2
import psycopg2.extras

#establish our connection to the database
connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
cursor.execute("""
    SELECT * FROM stock """)
rows = cursor.fetchall()

##create a dictionary for our stock
stocks = {}
for row in rows:
    stocks['$' +row[symbol]] = row[id]

#connect to the pushshift API
api = PushshiftAPI()

# change the date that you want to extract data from
start_time = int(datetime.datetime(2021, 2, 16).timestamp())

#filter how you want to see the posts
submissions = api.search_submissions(after=start_time,
                            subreddit='wallstreetbets',
                            filter=['url','author', 'title', 'subreddit'],)

for submission in submissions:
    #print(submission.created_utc)
    #print(submission.title)
    #print(submission.url)

# printing out all the posts with the cashtag in the post for example $GME
    words = submission.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'), words)))

    if len(cashtags) > 0:
        print(cashtags)
        print(submission.title)

        for cashtag in cashtags:
            if cashtag in stocks:
                submitted_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat()

                try:
                    cursor.execute("""
                        INSERT INTO mention (dt, stock_id, message, source, url)
                        VALUES (%s, %s, %s, 'wallstreetbets', %s)
                        """, (submitted_time, stocks[cashtag], submission.title, submission.url))

                    connection.commit()
                except Exception as e:
                    print(e)
                    connection.rollback()