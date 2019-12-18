# HashtagMicroservice

The purpose of this cute little microservice is to track all the hashtags in the application system and the corresponding 
lists of attached timelapses. 

In order to access all the hashtag "stuff" one should look for desired info in a particular Kafka topic, which is 
configured in conf/config.json (field "output_topic"). Each record in that topic represents a key-value pair, where
the key of type String is a certain hashtag and the value is a JsonArray of String IDs of all attached timelapses to 
that hashtag.
