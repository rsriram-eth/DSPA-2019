"""
Preprocessing task: DSPA-2019 : step 2 : could take 15 min (on 1k database)
Add the postId before the streaming is done on the replies. This is an assumption made that postId is already present on reply events to save processing time during stream execution.
Look-up and add PostId through the hierarchy
"""

import pandas as pd
import numpy as np

import threading

# Common variables

IP_DIR = "ip_data/1k-users-sorted/streams/"  # Input directory
MED_DIR = "scraps/"  # Intermediate files
OP_DIR = "op_data/"  # Final output directory
BL_DIR = "blacklist/"  # Blacklisted files
NUM_THREADS = 35

comment_df = pd.read_csv(MED_DIR + "purged_comment_stream.csv", header=0, sep="|")
print(comment_df.head(n=3))

"""
Add post_id to all replies - assumed that post_id information is available in a reply object
"""
print(comment_df['reply_to_postId'].isnull().sum(axis=0))  # Number of NaN values

missing_value_index = list(np.where(comment_df['reply_to_postId'].isnull())[0])
missing_value_ids = comment_df.id[missing_value_index]
print(len(missing_value_ids))

# Reset few column's data types
# print(comment_df.dtypes)
comment_df['reply_to_postId'] = comment_df['reply_to_postId'].fillna(-1).astype(np.int64)
comment_df['reply_to_commentId'] = comment_df['reply_to_commentId'].fillna(-1).astype(np.int64)


"""
Every thread executing this function handles len(missing_value_slice) comments that need to be filled.
Want to perform this task in parallel
"""


def add_post_id(missing_value_slice, set_num):
    count = 0
    for index in missing_value_slice:
        temp_row = comment_df[comment_df['id'] == index]
        row_index = np.where(comment_df['id'] == index)[0]
        temp_id = temp_row['reply_to_postId'].values  # must be -1 here

        while temp_id == -1:
            temp_row = comment_df.iloc[np.where(comment_df['id'] == temp_row['reply_to_commentId'].values[0])[0], :]
            temp_id = temp_row['reply_to_postId'].values

        # Add post id
        comment_df.at[row_index[0], 'reply_to_postId'] = temp_id
        count += 1
        print(count, set_num)

        if count == len(missing_value_slice):
            break

    print("Set number " + set_num + " done ....\n")
    return


class myThread (threading.Thread):
    def __init__(self, slice, set_num):
        threading.Thread.__init__(self)
        self.slice = slice
        self.set_num = set_num

    def run(self):
        print("\nStarting " + self.set_num)
        add_post_id(self.slice, self.set_num)
        print("\nExiting " + self.set_num)


# Divide the missing_values_list into equal chunks such that each thread can handle one chunk
chunks = np.array_split(missing_value_ids, NUM_THREADS)
"""
sum = 0
for x in chunks:
    sum += len(x)
    print(len(x))
print(sum)
"""

# Create new threads
thread = [myThread(chunks[i], str(i)) for i in range(NUM_THREADS)]

# Start new Threads
for i in range(NUM_THREADS):
    thread[i].start()

threads = []
for i in range(NUM_THREADS):
    threads.append(thread[i])

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Check
check_list = list(np.where(comment_df['reply_to_postId'] == -1)[0])
print(len(check_list))  # must be 0

# BUG: discrepancy due to threads
missing_value_index = list(np.where(comment_df['reply_to_postId'] == -1)[0])
missing_value_ids = comment_df.id[missing_value_index]
print(len(missing_value_ids))
# Without threading
add_post_id(missing_value_ids, "bug")
# Check again
check_list = list(np.where(comment_df['reply_to_postId'] == -1)[0])
print(len(check_list))  # must be 0

# Save final results
print("Comment_df final shape: ", comment_df.shape)
comment_df.to_csv(OP_DIR + "comment_stream.csv", sep='|', index=False)
