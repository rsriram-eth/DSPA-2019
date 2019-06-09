package poststats.datatypes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class StatEvent {
    Long postId;
    Integer commentCount;
    Integer replyCount;
    Integer likeCount;
    Integer uniquePeopleCount;
    Long lastActivityTimestamp;
    String activityTime;
    Long windowStartTimestamp;
    Long windowEndTimestamp;

    public StatEvent(){
        //For deserializer
    }

    public StatEvent(Long pId, Integer cCount, Integer rCount, Integer lCount, Integer uCount, Long lastTS,
                     Long windowStartTS, Long windowEndTS)
    {
        setPostId(pId);
        setCommentCount(cCount);
        setReplyCount(rCount);
        setLikeCount(lCount);
        setUniquePeopleCount(uCount);
        setLastActivityTimestamp(lastTS);
        setWindowStartTimestamp(windowStartTS);
        setWindowEndTimestamp(windowEndTS);

    }

    // GETTERS

    public Long getPostId() {
        return postId;
    }

    public Integer getCommentCount() {
        return commentCount;
    }

    public Integer getReplyCount() {
        return replyCount;
    }

    public Integer getLikeCount() {
        return likeCount;
    }

    public Integer getUniquePeopleCount() {
        return uniquePeopleCount;
    }

    public Long getLastActivityTimestamp() {
        return lastActivityTimestamp;
    }

    public String getActivityTime() {
        return activityTime;
    }

    public Long getWindowStartTimestamp() {
        return windowStartTimestamp;
    }

    public Long getWindowEndTimestamp() {
        return windowEndTimestamp;
    }

    // SETTERS

    public void setPostId(Long postId) {
        this.postId = postId;
    }

    public void setCommentCount(Integer commentCount) {
        this.commentCount = commentCount;
    }

    public void setReplyCount(Integer replyCount) {
        this.replyCount = replyCount;
    }

    public void setLikeCount(Integer likeCount) {
        this.likeCount = likeCount;
    }

    public void setUniquePeopleCount(Integer uniquePeopleCount) {
        this.uniquePeopleCount = uniquePeopleCount;
    }

    public void setLastActivityTimestamp(Long lastActivityTimestamp) {
        this.lastActivityTimestamp = lastActivityTimestamp;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        setActivityTime(sdf.format(new Date(lastActivityTimestamp)));
    }

    public void setActivityTime(String activityTime) {
        this.activityTime = activityTime;
    }

    public void setWindowStartTimestamp(Long windowStartTimestamp) {
        this.windowStartTimestamp = windowStartTimestamp;
    }

    public void setWindowEndTimestamp(Long windowEndTimestamp) {
        this.windowEndTimestamp = windowEndTimestamp;
    }

    /*
            @Override
            public String toString() {
                return "StatEvent{" +
                        "postId=" + postId +
                        ", commentCount=" + commentCount + '\'' +
                        ", replyCount=" + replyCount + '\'' +
                        ", likeCount=" + likeCount + '\'' +
                        ", uniquePeopleCount=" + uniquePeopleCount + '\'' +
                        ", timestamp='" + lastActivityTimestamp + '\'' +
                        ", time='" + activityTime + '\'' +
                        ", windowStart='" + windowStartTimestamp + '\'' +
                        ", windowEnd='" + windowEndTimestamp + '\'' +
                        '}';
            }
            */
    public String toString() {
        return "StatEvent{" +
                "S: " + windowStartTimestamp +
                ", E: " + windowEndTimestamp +
                ", postId=" + postId +
                ", " + commentCount +
                ", " + replyCount +
                ", " + likeCount +
                ", " + uniquePeopleCount +
                ", " + lastActivityTimestamp +
                ", " + activityTime +
                '}';
    }

}

