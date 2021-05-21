"""
A simple modules that performs some data cleaning tasks:
1) Delete all the redundant elements
2) Delete possible parallel edges
"""
import pandas as pd
import numpy as np
import gc

def clean_data(df,col_name_1):
    """
    This function deletes from a dataframe redundand posts. If a row contains
    a value already present in a DataFrame column chosen by the user, it will be deleted.

    Parameters
    ----------
    df:  pd.DataFrame
        Dataframe containig duplicate data
    col_name_1: string
        The name of the column in which duplicate values are deleted

    Yields
    ------
    df: pd.DataFrame
        A new DataFrame without duplicate ids
    """
    # Remove rows wich contain id we have already analyzed
    print("Number of columns before data cleaning: {}".format(df.shape[0]))
    df.drop_duplicates(subset=col_name_1,inplace=True)
    df.dropna(inplace=True)
    print("Number of columns after data cleaning: {}".format(df.shape[0]))
    df.reset_index(inplace=True,drop=True)
    return df

def remove_post(df,col_name_1,col_name_2):
    """
    This function deletes from a DataFrame all the rows having the same value in
    2 columns chosen by the user.
    Parameters
    ----------
    df:  pd.DataFrame
        Dataframe containig original posts
    col_name_1: string
        The name of the first column
    col_name_2: string
        The name of the second column

    Yields
    ------
    df: pd.DataFrame
        A new DataFrame without them
    """
    # check the subreddit in from and to is the same
    df_1 = df[df[col_name_1] == df[col_name_2]]
    # delete duplicate rows from dataframe
    new_df = df.drop(df_1.index)
    #delete useless data to free memory
    del df,df_1
    gc.collect()
    #reset the indexes in the new DataFrame
    new_df.reset_index(inplace=True,drop=True)
    return new_df

def create_direct(df):
    """
    Since many graoh algorithm does not work with parallel edges ths function transforms
    the subreddit dataframe to make sure it works best to create a direct graph.
    This function eliminates all possible parallel edges and saves:
    1) The number of parallel edges.
    2) The sum of the number of comments and upvotes among the parallel edges
    3) The date of the oldest among the parallel edges.

    Parameters
    ----------
    df:  pd.DataFrame
        The subreddit DataFrame

    Yields
    ------
    new_df: pd.DataFrame
        A new DataFrame without the parallel edges
    """
    dict_direct = {}
    new_parents = []
    new_sons = []
    interactions = []
    crossposts = []
    dates = []
    print("Transforming DataFrame...")
    # create a list of all the unique parents
    lst_parents = list(pd.unique(df["parent"]))
    # find all the crosspost for each parent
    for i,parent in enumerate(lst_parents):
        multi_link = df[df["parent"] == parent]
        lst_sons = list(pd.unique(multi_link["to"]))
        # Compress the data
        for son in lst_sons:
            multi_link_2 = multi_link[multi_link["to"] == son]
            new_parents.append(parent)
            new_sons.append(son)
            interactions.append((multi_link_2["comments"]+multi_link_2["score"]).sum())
            crossposts.append(len(multi_link_2))
            dates.append(multi_link_2["date"].min())
        # check the progress each 500 interactions
        if i%500 == 0:
            print("{} parents remaining".format(len(lst_parents)-i))
    # save the results as DataFrame
    dict_direct["parent"] = new_parents
    dict_direct["to"] = new_sons
    dict_direct["interactions"] =  interactions
    dict_direct["crossposts"] = crossposts
    dict_direct["date"] = dates
    new_df = pd.DataFrame(dict_direct)
    return new_df

if __name__=="__main__":

    # load data and reset indexes
    data = pd.read_csv("data/data_subreddit_raw.csv",index_col=0)
    data.reset_index(inplace=True,drop=True)
    # save raw data with reset indexes
    data.to_csv("data/data_subreddit_raw.csv")
    # remove duplicates
    clean_data = clean_data(data,"id")
    clean_data = remove_post(data,"parent","to")
    # save data without duplicates
    clean_data.to_csv("data/data_subreddit_cleaned.csv")
    # transform the DataFrame to suit direct graph
    direct_data = create_direct(clean_data)
    direct_data["weights"] = np.log(direct_data["interactions"]+1)
    direct_data.to_csv("data/data_subreddit_direct.csv")
