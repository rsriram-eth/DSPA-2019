package poststats.datatypes;

import java.time.Instant;

public class CommonEvent {
    String creationDate;
    long timestamp;
    Integer personId;
    Character eventType;
    long postId;

    // Getters

    public String getCreationDate() {
        return creationDate;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Character getEventType() {
        return eventType;
    }

    public Integer getPersonId() {
        return personId;
    }

    public long getPostId() {
        return postId;
    }

    // Setters

    public void getMilliseconds() {
        if(this.creationDate != null)
            this.timestamp = Instant.parse( this.creationDate ).toEpochMilli();
        // System.out.println(timestamp);
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        getMilliseconds();
    }

    public void setEventType(Character eventType) {
        this.eventType = eventType;
    }

    public void setPersonId(Integer personId) {
        this.personId = personId;
    }

    public void setPostId(long postId) {
        this.postId = postId;
    }
}