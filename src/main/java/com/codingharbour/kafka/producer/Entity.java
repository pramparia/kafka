package com.codingharbour.kafka.producer;

import com.alibaba.fastjson.JSON;

/**
 * {
 *   "event_type": "01",
 *   "event_name": "video_like",
 *   "date": "2023-11-01",
 *   "time": 1700328589082,
 *
 *   "user_id": "log-7",
 *   "user_age": 32,
 *   "user_gender": "male",
 *   "user_province": "canada",
 *
 *   "video_id": "v_122",
 *   "video_type": "sport",
 *   "video_subtype": "football",
 *   "video_duration": 120,
 *   "author_id": "author-7"
 * }
 */
public class Entity {

    private String eventType;
    private String eventName;
    private String date;
    private Long time;

    private String userId;
    private Integer userAge;
    private String userGender;
    private String userProvince;

    private String videoId;
    private String videoSubject;
    private String videoSubSubject;
    private Integer videoDuration;

    private Integer videoPlayedDuration;
    private Integer videoPlayedTimes;
    private String authorId;
    private Integer playDuration;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getUserAge() {
        return userAge;
    }

    public void setUserAge(Integer userAge) {
        this.userAge = userAge;
    }

    public String getUserGender() {
        return userGender;
    }

    public void setUserGender(String userGender) {
        this.userGender = userGender;
    }

    public String getUserProvince() {
        return userProvince;
    }

    public void setUserProvince(String userProvince) {
        this.userProvince = userProvince;
    }

    public String getVideoId() {
        return videoId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public String getVideoSubject() {
        return videoSubject;
    }

    public void setVideoSubject(String videoSubject) {
        this.videoSubject = videoSubject;
    }

    public String getVideoSubSubject() {
        return videoSubSubject;
    }

    public void setVideoSubSubject(String videoSubSubject) {
        this.videoSubSubject = videoSubSubject;
    }

    public Integer getVideoDuration() {
        return videoDuration;
    }

    public void setVideoDuration(Integer videoDuration) {
        this.videoDuration = videoDuration;
    }

    public String getAuthorId() {
        return authorId;
    }

    public void setAuthorId(String authorId) {
        this.authorId = authorId;
    }

    public Integer getPlayDuration() {
        return playDuration;
    }

    public void setPlayDuration(Integer playDuration) {
        this.playDuration = playDuration;
    }

    public Integer getVideoPlayedDuration() {
        return videoPlayedDuration;
    }

    public void setVideoPlayedDuration(Integer videoPlayedDuration) {
        this.videoPlayedDuration = videoPlayedDuration;
    }

    public Integer getVideoPlayedTimes() {
        return videoPlayedTimes;
    }

    public void setVideoPlayedTimes(Integer videoPlayedTimes) {
        this.videoPlayedTimes = videoPlayedTimes;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
