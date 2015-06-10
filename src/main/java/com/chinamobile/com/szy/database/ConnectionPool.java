package com.chinamobile.com.szy.database;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Administrator on 15-6-10.
 */
public class ConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    private static ComboPooledDataSource _ds = null;
    private static ReentrantLock lock = new ReentrantLock();

    public static void build(String jdbcUrl, String username, String password, int poolSize, int maxPoolSize, int maxIdleTime){
        initializePool(jdbcUrl,username, password, poolSize, maxPoolSize, maxIdleTime);
    }

    private static void initializePool(String jdbcUrl, String username, String password, int poolSize,
                                       int maxPoolSize, int maxIdleTime){
        try {
            lock.lock();
            if(_ds != null){
                return;
            }
            _ds = new ComboPooledDataSource();
            _ds.setUser(username);
            _ds.setPassword(password);
            _ds.setInitialPoolSize(poolSize);
            _ds.setMaxPoolSize(maxPoolSize);
            _ds.setMaxIdleTime(maxIdleTime);
            _ds.setJdbcUrl(jdbcUrl);
        }catch (Exception e){
            logger.error("initialize oracle connection pool exception, {}",e);
        }finally {
            lock.unlock();
        }
    }

    public static Connection getConnection(){
        Connection connection = null;
        try {
            connection = _ds.getConnection();
        }catch (Exception e){
            logger.error("get oracle connection exception,{}", e);
        }
        return connection;
    }

    public static void close(){
        _ds.close();
    }
}
