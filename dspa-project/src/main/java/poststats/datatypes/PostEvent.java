package poststats.datatypes;

public class PostEvent extends CommonEvent {

    public PostEvent(String cDate, Integer perId, Character eType, long pId){
        setCreationDate(cDate);
        setPersonId(perId);
        setEventType(eType);
        setPostId(pId);
    }

    public PostEvent(){
        // empty constructor needed for object deserialization code
    }

    @Override
    public String toString() {
        return "PostEvent{" +
                "timestamp=" + timestamp +
                ", personId='" + personId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", postId='" + postId + '\'' +
                '}';
    }
}
