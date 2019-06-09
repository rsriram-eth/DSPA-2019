"""
Preprocessing task: DSPA-2019 : STEP 1
Read appropriate columns + blacklist removal
"""

import pandas as pd
import numpy as np

# Common variables

IP_DIR = "ip_data/1k-users-sorted/streams/"  # Input directory
MED_DIR = "scraps/"  # Intermediate files
OP_DIR = "op_data/"  # Final output directory
BL_DIR = "blacklist/"  # Blacklisted files

"""
* Read csv to dataframe
* Uniform naming
* Drop unwanted columns
"""
post_df = pd.read_csv(IP_DIR + "post_event_stream.csv", header=0, sep="|",
                      usecols=['id', 'personId', 'creationDate', 'locationIP',
                               'content', 'tags', 'forumId', 'placeId']
                      )

comment_df = pd.read_csv(IP_DIR + "comment_event_stream.csv", header=0, sep="|",
                         usecols=['id', 'personId', 'creationDate',
                                  'locationIP', 'content', 'reply_to_postId',
                                  'reply_to_commentId', 'placeId']
                         )

like_df = pd.read_csv(IP_DIR + "likes_event_stream.csv", header=0, sep="|",
                      names=['personId', 'postId', 'creationDate']
                      )

# Check: Preview the first 5 lines of the loaded data
print(post_df.head())
print(comment_df.head())
print(like_df.head())

# Print one row
# print(post_df.iloc[1, :])

"""
Purge blacklisted comments
"""


def get_all_ids_to_purge(df, blacklist):
    remove_ids = []
    id = 0

    while id != len(blacklist):
        # Remove this comment from comment_df
        temp = list(np.where(df['id'] == blacklist[id])[0])

        # Remove any sub-tree below this faulty comment
        replies = list(np.where(df['reply_to_commentId'] == blacklist[id])[0])

        if len(replies):
            # Add replies to blacklist to purge whole affected subtree
            blacklist = blacklist + list(df.id[replies])

        # Row indexes to drop
        remove_ids = remove_ids + temp
        id += 1
        print(len(blacklist), id)

    # Only unique ids
    # print(remove_ids)
    unique = list(set(remove_ids))
    print(len(remove_ids), len(unique))
    return unique


# Read list from csv
black_df = pd.read_csv(BL_DIR + "1k-blacklist.csv", header=0, sep="|")
# print(black_df.head())

print("\nBefore purge : " + str(comment_df.shape))
remove_list = get_all_ids_to_purge(comment_df, list(black_df['comment_id']))
print("Removing " + str(len(remove_list)) + " comments .... ")
comment_df = comment_df.drop(remove_list, axis=0)
print("After purge : " + str(comment_df.shape))

comment_df.to_csv(MED_DIR + "purged_comment_stream.csv", sep='|', index=False)
