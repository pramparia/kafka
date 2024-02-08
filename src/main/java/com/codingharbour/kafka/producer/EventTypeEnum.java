package com.codingharbour.kafka.producer;

public enum EventTypeEnum {

    VIDEO_EXPOSURE(200, "video_exposure"),
    USER_VIDEO_WATCH_DURATION(60, "user_video_watch_duration"),
    VIDEO_LIKE(30, "video_like"),
    VIDEO_SHARE(20, "video_share"),
    VIDEO_COMMENT(18, "video_comment"),
    VIDEO_FAVOURITE(12, "video_favourite"),
    VIDEO_DISLIKE(16, "video_dislike"),
    VIDEO_REPORT(8, "video_report"),
    USER_FOLLOW(8, "user_follow"),
    USER_UNFOLLOW(6, "user_unfollow"),
    USER_SEND_MSG(11, "user_send_msg");

    private Integer eventId;
    private String eventName;

    EventTypeEnum(Integer eventId, String eventName) {
        this.eventId = eventId;
        this.eventName = eventName;
    }


    public Integer getEventId() {
        return eventId;
    }

    public void setEventId(Integer eventId) {
        this.eventId = eventId;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }
}
