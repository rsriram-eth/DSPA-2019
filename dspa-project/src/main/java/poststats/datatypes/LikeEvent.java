package poststats.datatypes;

public class LikeEvent extends CommonEvent {

    public LikeEvent(String cDate, Integer perId, Character eType, long pId){
        setCreationDate(cDate);
        setPersonId(perId);
        setEventType(eType);
        setPostId(pId);
    }

    public LikeEvent(){
        // empty constructor needed for object deserialization code
    }

    @Override
    public String toString() {
        return "LikeEvent{" +
                "timestamp=" + timestamp +
                ", personId='" + personId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", postId='" + postId + '\'' +
                '}';
    }

}
