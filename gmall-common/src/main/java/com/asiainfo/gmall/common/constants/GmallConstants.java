package com.asiainfo.gmall.common.constants;

public class GmallConstants {

public static final String KAFKA_TOPIC_STARTUP="GMALL_STARTUP";
public static final String KAFKA_TOPIC_EVENT="GMALL_EVENT";
public static final String KAFKA_CONSUMER_GROUP="gmall_consumer_group";
public static final String KAFKA_TOPIC_NEW_ORDER="GMALL_NEW_ORDER";
public static final String KAFKA_TOPIC_ORDER_INFO="GMALL_ORDER_INFO";
public static final String KAFKA_TOPIC_USER_INFO="GMALL_USER_INFO";
public static final String KAFKA_TOPIC_ORDER_DETAIL="GMALL_ORDER_DETAIL";
public static final String KAFKA_BOOTSTRAP_SERVER="s102:9092,s103:9092,s104:9092";
public static final String KAFKA_SERIALIZER="org.apache.kafka.common.serialization.StringSerializer";

public static final String ZKURL="s102,s103,s104:2181";
public static final String CLASSPATH_CONFIG="config.properties";
public static final int FULLOUTERJOIN_REDIS_EXPIRE=5*60;
public static final String FULLOUTERJOIN_REDIS_ORDERINFOKEY_PRE="order_info:";
public static final String FULLOUTERJOIN_REDIS_ORDERDETAILKEY_PRE="order_detail:";
public static final String FULLOUTERJOIN_REDIS_USERINFOKEY_PRE="user_info:";
//canal 关注的数据库变化
public static final String CANAL_WATCH_DB="gmall_2019.*";
public static final String PHOENIX_ORDER_INFO="\"gmall2019_order_info\"";
public static final String PHOENIX_USER_INFO="gmall2019_user_info";

public static final String ES_INDEX_DAU="gmall2019_dau";
public static final String ES_INDEX_NEW_MID="gmall2019_new_mid";
public static final String ES_INDEX_NEW_ORDER="gmall2019_new_order";
public static final String ES_INDEX_SALE_DETAIL="gmall2019_sale_detail";

public static final String ES_INDEX_COUPON_ALERT="gmall2019_coupon_alert";
//如果要以下划线开头，必须是这个名字,常量修改后，不一定立马生效，有时候可能没有编译，导致出错，最好是刷新一下
public static final String ES_INDEX_COUPON_ALERT_TYPE="_doc";
public static final String ES_QUERY_AGGREGATION_GENDER="groupByGender";
public static final String ES_QUERY_AGGREGATION_USERAGE="groupByAge";


}
