"""
Preprocessing task: DSPA-2019 : step 3 : purge likes with original timestamp earlier than their post
"""

import pandas as pd
import numpy as np
import dateutil.parser

# Common variables

IP_DIR = "ip_data/1k-users-sorted/streams/"  # Input directory
MED_DIR = "scraps/"  # Intermediate files
OP_DIR = "op_data/"  # Final output directory
BL_DIR = "blacklist/"  # Blacklisted files
NUM_THREADS = 35

post_df = pd.read_csv(IP_DIR + "post_event_stream.csv", header=0, sep="|",
                      usecols=['id', 'creationDate']
                      )

like_df = pd.read_csv(IP_DIR + "likes_event_stream.csv", header=0, sep="|",
                      names=['personId', 'postId', 'creationDate']
                      )

print(post_df.head(n=1))
print(like_df.head(n=1))

"""
Delete likes which are earlier than their post time
"""

num_likes = like_df.shape[0]
print("Original number of likes:", num_likes)

remove_ids = []

for index in range(num_likes):
    like_time = like_df.at[index, 'creationDate']  # ISO 8601 format
    like_time = dateutil.parser.parse(like_time)
    # print("\nLike time: ", like_time)
    post_id = like_df.at[index, 'postId']
    # print("Post id: ", post_id)
    post_row = np.where(post_df['id'] == post_id)[0]  # in post_df
    # print("Post row: ", post_row)
    post_time = post_df.at[post_row[0], 'creationDate']
    post_time = dateutil.parser.parse(post_time)
    # print("Post time: ", post_time)

    if like_time <= post_time:  # then drop
        remove_ids.append(index)
        print("\nLike time: ", like_time)
        print("Post time: ", post_time)
        print(index)

# print(remove_ids)
print("\nRemoving " + str(len(remove_ids)) + " comments .... ")
like_df = like_df.drop(remove_ids, axis=0)
num_likes = like_df.shape[0]
print("Final number of likes:", num_likes)

# Save cleaned results
like_df.to_csv(OP_DIR + "like_stream.csv", sep='|', index=False)
