package com.chinamobile.com.szy.database;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by Administrator on 15-6-10.
 */
public class DBHelper {
    private static final Logger logger = LoggerFactory.getLogger(DBHelper.class);


    public DBHelper(String jdbcUrl, String username, String password, int poolSize, int maxPoolSize, int maxIdleTime) {
        ConnectionPool.build(jdbcUrl, username, password, poolSize, maxPoolSize, maxIdleTime);
    }

    /**
     * @param table
     * @param startRowNum
     * @param endNumber
     * @param columns
     * @return
     */
    public List<JSONObject> query(String table, int startRowNum, int endNumber, Set<String> columns) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select * from (select ROWNUM AS ROWNO, ");
        for (String column : columns) {
            sqlBuilder.append(column).append(",");
        }
        sqlBuilder = new StringBuilder(sqlBuilder.toString().substring(0, sqlBuilder.length() - 1));
        sqlBuilder.append("  from ").append(table).append(" where ROWNUM <").append(endNumber).append(") " +
                "TABLE_ALIAS where TABLE_ALIAS.ROWNO >=").append(startRowNum);

        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement statement = null;
        try {
            List<JSONObject> list = new ArrayList<JSONObject>();
            connection = ConnectionPool.getConnection();
            statement = connection.prepareStatement(sqlBuilder.toString());
            resultSet = statement.executeQuery();
            JSONArray jsonArray = new JSONArray();
            for (String column : columns) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(column, resultSet.getObject(column));
                list.add(jsonObject);
            }
            return list;
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    logger.error("", e);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("", e);

                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                    }
                }
            }
        }
        return new ArrayList<JSONObject>();
    }

    public void close(){
        ConnectionPool.close();
    }
}
