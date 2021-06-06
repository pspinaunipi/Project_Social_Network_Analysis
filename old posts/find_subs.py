"""
Per ottenere il numero di subs 
"""
import praw
import pandas as pd
import sys

if __name__ == "__main__":

    reddit = praw.Reddit(client_id='uZBIHm378Zr5Zg', client_secret='gRZpYGtljXSwmYZ006Kog1GHWa-ISg', user_agent='betta1309')
    
    file = "subs_unique_pre2021.txt"
    
    
    subs = {}
    
    # subreddits to score
    with open(file, "r", encoding='utf-8') as f:
        data_sub = [line.rstrip() for line in f]
    popped = []
    failed = []
    # find the subscribers for each subreddit
    
    for name in data_sub:
        try:
            subreddit1 = reddit.subreddit(name)
            subscribers = subreddit1.subscribers
            print(name, ": ", subscribers)
            subs[name]=subscribers
            popped.append(name)
        except KeyboardInterrupt:
            sys.exit()
        except:
            failed.append(name)
        

    # create a DataFrame from the dict
    df = pd.DataFrame.from_dict(subs, orient='index')

    # unstack, drop na and sort
    print("Falliti alla fine:")
    print(failed)
    df.unstack().dropna().sort_index(level=1)
    print(df)
    df.to_csv("subscribers_postpre2021.csv")
