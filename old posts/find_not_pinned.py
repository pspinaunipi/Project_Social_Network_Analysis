"""
Per ottenere la lista dei post pinned e di quelli non pinned
"""
import praw
import pandas as pd
import sys

if __name__ == "__main__":

    reddit = praw.Reddit(client_id='uZBIHm378Zr5Zg', client_secret='gRZpYGtljXSwmYZ006Kog1GHWa-ISg', user_agent='betta1309')
    
    file = "post_pre_2021_ordinati.csv"
    
    crossposts = pd.read_csv(file)
    
    # array di booleani per salvare se un post è pinned (true) o non lo è
    pinned = []
    failed = []
    
    post_da_controllare = [[x, y] for x, y in zip(crossposts['from'], crossposts['id'])]
    
    for sub, id_post in post_da_controllare:
        try:
            post = reddit.submission(id=id_post)
            stickied  = post.stickied
            print(sub, id_post, stickied)
            pinned.append(stickied)
        except KeyboardInterrupt:
            sys.exit()
        except:
            print(sub, id_post, " errore")
            failed.append([sub,id_post])
            pinned.append(None)

    print(failed)
    crossposts["pinned"]=pinned
    crossposts.to_csv("pre_2021_pinned.csv")
    