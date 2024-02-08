package com.codingharbour.kafka.producer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

public class SimpleProducer {


    private static List<ArrayList<String>> userList = new ArrayList<ArrayList<String>>() {{
        for (ProvinceEnum provinceEnum : ProvinceEnum.values()) {
            Integer weight = Long.valueOf(provinceEnum.getPopulation() * 100 / 40000000L).intValue();
            Random random = new Random();
            if (weight < 1) {
                weight = 1;
            }
            List<String> genderList = new ArrayList<String>() {{
                this.add("Male");
                this.add("Female");
            }};
            for (int i = 0; i < weight; i++) {
                int finalI = i;
                /**
                 * "user_id": "log-7",
                 *   "user_age": 32,
                 *   "user_gender": "male",
                 *   "user_province": "canada",
                 */
                this.add(new ArrayList() {{
                    this.add(String.format("user_%s_%d", provinceEnum.getProvinceName(), finalI));
                    this.add(random.nextInt(80) + 15 + "");
                    this.add(genderList.get(random.nextInt(2)));
                    this.add(provinceEnum.getProvinceName());
                }});
            }
        }
    }};

    private static List<ArrayList<String>> videoList = new ArrayList<ArrayList<String>>() {{
        for (VideoTypeEnum videoTypeEnum : VideoTypeEnum.values()) {
            Random random = new Random();
            Integer weight = random.nextInt(5) + 1;
            for (int i = 0; i < weight; i++) {
                int finalI = i;
                /**
                 *  "video_id": "v_122",
                 *   "video_type": "sport",
                 *   "video_subtype": "football",
                 *   "video_duration": 120,
                 *   "author_id": "author-7"
                 *   "videoPlayedDuration":200,
                 *   "videoPlayedTimes":1.5,
                 */
                this.add(new ArrayList() {{
                    this.add(String.format("video_%s_%s_%d", videoTypeEnum.getVideoType(), videoTypeEnum.getVideoSubtype(), finalI));
                    this.add(videoTypeEnum.getVideoType());
                    this.add(videoTypeEnum.getVideoSubtype());
                    int duration = random.nextInt(400) + 30;
                    this.add(duration + "");
                    this.add(String.format("author_%s_%s_%d", videoTypeEnum.getVideoType(), videoTypeEnum.getVideoSubtype(), finalI));
                    int playedTimes = random.nextInt(10) + 1;
                    this.add(duration * playedTimes + "");
                    this.add(playedTimes + "");
                }});
            }
        }
    }};

    public static void main(String[] args) throws Exception {

        System.out.println(String.format("args:%s",JSON.toJSONString(args)));
        // Check arguments length value
        if (args.length == 0) {
            System.out.println("Enter args para");
            return;
        }
        Integer produceFrequency = Integer.valueOf(args[0]);
        String kafkaServer = args[1];

        //Assign topicName to string variable
        String topicName = "test";

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", kafkaServer);

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        while (true) {
            List<Entity> entityList = generateMsg();
            for (Entity entity : entityList) {
                producer.send(new ProducerRecord<String, String>(topicName, entity.getEventName(), JSON.toJSONString(entity)));
            }
            System.out.println("Message sent successfully:" + new Date());
            Thread.sleep((1 + new Random().nextInt(produceFrequency)) * 1000L);
        }
    }

    private static List<Entity> generateMsg() {
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
        ArrayList<Entity> entityList = new ArrayList<>();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String date = simpleDateFormat.format(new Date());
        int userListSize = userList.size();
        int videoListSize = videoList.size();
        Random random = new Random();
        for (EventTypeEnum eventTypeEnum : EventTypeEnum.values()) {
            int eventWeight  = Double.valueOf(eventTypeEnum.getEventId()*5/(random.nextInt(10)+1.0)).intValue();
            for (int i = 0; i < eventWeight; i++) {
//                if(entityList.size()>5){
//                    break;
//                }
                Entity entity = new Entity();
                entity.setEventName(eventTypeEnum.getEventName());
                entity.setDate(date);
                entity.setTime(System.currentTimeMillis());

                ArrayList<String> userPropertyList = userList.get(random.nextInt(userListSize));
                entity.setUserId(userPropertyList.get(0));
                entity.setUserAge(Integer.valueOf(userPropertyList.get(1)));
                entity.setUserGender(userPropertyList.get(2));
                entity.setUserProvince(userPropertyList.get(3));

                ArrayList<String> videoPropertyList = videoList.get(random.nextInt(videoListSize));
                if (eventTypeEnum.getEventName().equals(EventTypeEnum.USER_FOLLOW.getEventName()) ||
                        eventTypeEnum.getEventName().equals(EventTypeEnum.USER_UNFOLLOW.getEventName()) ||
                        eventTypeEnum.getEventName().equals(EventTypeEnum.USER_SEND_MSG.getEventName())) {
                } else {
                    entity.setVideoId(videoPropertyList.get(0));
                    entity.setVideoSubject(videoPropertyList.get(1));
                    entity.setVideoSubSubject(videoPropertyList.get(2));
                    entity.setVideoDuration(Integer.valueOf(videoPropertyList.get(3)));
                    entity.setAuthorId(videoPropertyList.get(4));
                }

                if (eventTypeEnum.getEventName().equals(EventTypeEnum.USER_VIDEO_WATCH_DURATION.getEventName())) {
                    entity.setVideoPlayedDuration(Integer.valueOf(videoPropertyList.get(5)));
                    entity.setVideoPlayedTimes(Integer.valueOf(videoPropertyList.get(6)));
                }
                entityList.add(entity);
            }
        }
        return entityList;

    }

}
