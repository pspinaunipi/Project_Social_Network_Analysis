"""
A simple modules that does some data cleaning tasks
"""
import pandas as pd
import gc

def clean_data(df):
    """
    This function deletes from a dataframe redundand posts. If a row contains
    a post id already present in the DataFrame, it will be deleted.

    Parameters
    ----------
    df:  pd.DataFrame
        Dataframe containig duplicate data

    Yields
    ------
    df: pd.DataFrame
        A new DataFrame without duplicate ids
    """
    # Remove rows wich contain id we have already analyzed
    print("Number of columns before data cleaning: {}".format(data.shape[0]))
    df.drop_duplicates(subset="id",inplace=True)
    df.dropna(inplace=True)
    print("Number of columns after data cleaning: {}".format(data.shape[0]))
    df.reset_index(inplace=True,drop=True)
    return df

def remove_original_post(df):
    """
    This function deletes from a DataFrame each row that has the same subreddit name
    in both the column "from" and the column "to"
    Parameters
    ----------
    df:  pd.DataFrame
        Dataframe containig original posts

    Yields
    ------
    df: pd.DataFrame
        A new DataFrame without them
    """
    # check the subreddit in from and to is the same
    df_1 = df[df["from"] == df["to"]]
    # delete duplicate rows from dataframe
    new_df = df.drop(df_1.index)
    #delete useless data to free memory
    del df,df_1
    gc.collect()
    #reset the indexes in the new DataFrame
    new_df.reset_index(inplace=True,drop=True)
    return new_df

if __name__=="__main__":

    # load data and reset indexes
    data = pd.read_csv("data/data_subreddit_raw.csv",index_col=0)
    data.reset_index(inplace=True,drop=True)
    # save raw data with reset indexes
    data.to_csv("data/data_subreddit_raw.csv")
    # remove duplicates
    clean_data = clean_data(data)
    # save data without duplicates
    clean_data.to_csv("data/data_subreddit_cleaned.csv")
    # remove original posts from raw data
    raw_no_og_post = remove_original_post(data)
    # save the results
    raw_no_og_post.to_csv("data/data_subreddit_raw_no_og.csv")
    # remove original posts from cleaned data
    clean_no_og_post = remove_original_post(data)
    # save the results
    clean_no_og_post.to_csv("data/data_subreddit_cleaned_no_og.csv")
