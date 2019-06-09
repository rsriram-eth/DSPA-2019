package poststats.datatypes;

public class CommentEvent extends CommonEvent {
    long commentId;
    long replyToCommentId;

    public CommentEvent(String cDate, Integer perId, Character eType, long pId, long cId, long rcId){
        setCreationDate(cDate);
        setPersonId(perId);
        setEventType(eType);
        setPostId(pId);
        setCommentId(cId);
        setReplyToCommentId(rcId);
    }

    public CommentEvent(){
        // empty constructor needed for object deserialization code
    }

    // Getters
    public long getCommentId() {
        return commentId;
    }

    public long getReplyToCommentId() {
        return replyToCommentId;
    }

    // Setters
    public void setCommentId(long commentId) {
        this.commentId = commentId;
    }

    public void setReplyToCommentId(long replyToCommentId) {
        this.replyToCommentId = replyToCommentId;
    }

    @Override
    public String toString() {
        return "CommentEvent{" +
                "timestamp=" + timestamp +
                ", personId='" + personId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", postId='" + postId + '\'' +
                ", commentId='" + commentId + '\'' +
                ", replyToCommentId=" + replyToCommentId +
                '}';
    }
}
