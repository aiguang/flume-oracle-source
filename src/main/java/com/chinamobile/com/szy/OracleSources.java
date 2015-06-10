package com.chinamobile.com.szy;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.chinamobile.com.szy.database.DBHelper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Administrator on 15-6-9.
 */
public class OracleSources extends AbstractSource implements EventDrivenSource, Configurable, PollableSource {

    private static final Logger logger = LoggerFactory.getLogger(OracleSources.class);

    private int startNum;
    private String table;

    private int NUM_STEP;
    private DBHelper dbHelper;
    private Set<String> columns;

    @Override
    public synchronized void start() {
        logger.info("oracle sources start");
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("oracle sources stop");
        super.stop();
        //连接池优雅关闭
        dbHelper.close();
    }

    @Override
    public void configure(Context context) {
        logger.info("oracle sources configure start");
        String username = context.getString("username");
        String password = context.getString("password");
        String jdbcUrl = context.getString("jdbcUrl");
        int poolSize = context.getString("poolSize") == null ? 2 : context.getInteger("poolSize");
        int maxPoolSize = context.getString("maxPoolSize") == null ? poolSize :
                context.getInteger("maxPoolSize");
        int maxIdleTime = context.getString("maxIdleTime") == null ? 30 :
                context.getInteger("maxIdleTime");

        table = context.getString("table");
        startNum = context.getInteger("startNum");
        NUM_STEP = context.getInteger("numStep");

        columns = Sets.newHashSet(context.getString("columns").split(","));
        dbHelper = new DBHelper(jdbcUrl, username, password, poolSize, maxPoolSize, maxIdleTime);
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            ArrayList<Event> eventList = new ArrayList<Event>();
            Event event;
            Map<String, String> headers;
            List<JSONObject> list = dbHelper.query(table, startNum, startNum + NUM_STEP, columns);
            for(JSONObject message : list){
                headers = new HashMap<String, String>();
                headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
                headers.put("table", table);
                headers.put("info", "oracle source");
                event = new SimpleEvent();
                byte[] bytes = message.toJSONString().getBytes();
                event.setBody(bytes);
                eventList.add(event);
            }
            getChannelProcessor().processEventBatch(eventList);
            return Status.READY;
        }catch (Exception e){
            logger.error("", e);
            return Status.BACKOFF;
        }
    }
}
