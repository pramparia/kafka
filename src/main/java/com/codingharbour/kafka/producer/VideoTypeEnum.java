package com.codingharbour.kafka.producer;

public enum VideoTypeEnum {

    EDUCATIONAL_TUTORIAL("101001","Educational","Tutorial"),
    EDUCATIONAL_ONLINE_COURSES("101002","Educational","Online Courses"),
    EDUCATIONAL_WEBINARS("101003","Educational","Webinars"),
    EDUCATIONAL_LECTURES("101004","Educational","Lectures"),

    ENTERTAINMENT_MOVIES("102001","Entertainment","Movies"),
    ENTERTAINMENT_TV_SHOWS("102002","Entertainment","TV shows"),
    ENTERTAINMENT_MUSIC_VIDEOS("102003","Entertainment","Music videos"),
    ENTERTAINMENT_COMEDY_SKITS("102004","Entertainment","Comedy skits"),

    NEWS_AND_DOCUMENTARY_DAILY_VLOGS("103001","News and Documentary","Daily Vlogs"),
    NEWS_AND_DOCUMENTARY_TRAVEL_VLOGS("103002","News and Documentary","Travel Vlogs"),
    NEWS_AND_DOCUMENTARY_LIFESTYLE_VLOGS("103003","News and Documentary","Lifestyle Vlogs"),
    NEWS_AND_DOCUMENTARY_CHALLENGE_VIDEOS("103004","News and Documentary","Challenge Videos"),

    GAMING_LETS_PLAY("104001","Gaming","Let's Play"),
    GAMING_WALKTHROUGHS("104002","Gaming","Walkthroughs"),
    GAMING_REVIEWS("104003","Gaming","Reviews"),
    GAMING_ESPORTS("104004","Gaming","eSports"),

    FITNESS_WORKOUT_ROUTINES("105001","Fitness","Workout Routines"),
    FITNESS_YOGA_AND_MEDITATION("105002","Fitness","Yoga and Meditation"),
    FITNESS_NUTRITION_GUIDES("105003","Fitness","Nutrition Guides"),
    FITNESS_HEALTH_TIPS("105004","Fitness","Health Tips"),

    INSTRUCTIONAL_DIY_PROJECTS("106001","instructional","DIY Projects"),
    INSTRUCTIONAL_COOKING_RECIPES("106002","instructional","Cooking Recipes"),
    INSTRUCTIONAL_HOME_IMPROVEMENT("106003","instructional","Home Improvement"),
    INSTRUCTIONAL_TECH_TUTORIALS("106004","instructional","Tech Tutorials");

    private String videoTypeId;
    private String videoType;
    private String videoSubtype;

    VideoTypeEnum(String videoTypeId, String videoType, String videoSubtype) {
        this.videoTypeId = videoTypeId;
        this.videoType = videoType;
        this.videoSubtype = videoSubtype;
    }

    public String getVideoTypeId() {
        return videoTypeId;
    }

    public void setVideoTypeId(String videoTypeId) {
        this.videoTypeId = videoTypeId;
    }

    public String getVideoType() {
        return videoType;
    }

    public void setVideoType(String videoType) {
        this.videoType = videoType;
    }

    public String getVideoSubtype() {
        return videoSubtype;
    }

    public void setVideoSubtype(String videoSubtype) {
        this.videoSubtype = videoSubtype;
    }
    }
