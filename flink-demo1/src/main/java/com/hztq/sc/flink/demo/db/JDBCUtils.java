package com.hztq.sc.flink.demo.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: 导出数据的JDBC配置
 * @author: liujun 249489478@qq.com
 * @create: 2019-07-11 09:44
 */
public class JDBCUtils {
    static Connection conn = null;
    static {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序

            String url = "jdbc:oracle:thin:@10.0.188.8:13521:sccenter";
            String pwd = "dczitong";
            String userName = "dczitong";
            conn = DriverManager.getConnection(url, userName, pwd);// 获取连接
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接方法
     *
     * @return
     */
    public static Connection getConnection() {
//        Connection conn = null;
//        try {
//            Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
//
//            String url = "jdbc:oracle:thin:@10.0.188.8:13521:sccenter";
//            String pwd = "dczitong";
//            String userName = "dczitong";
//            conn = DriverManager.getConnection(url, userName, pwd);// 获取连接
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        return conn;
    }

    public static void release(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static List<Map<Object,Object>> query(String sql, Connection conn){
        PreparedStatement pstmt = null;
        List<Map<Object,Object>> list = new ArrayList<>();
        //1.创建自定义连接池对象
        try {
            //2.从池子中获取连接
            pstmt = conn.prepareStatement(sql);
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()){
                Map<Object,Object> map = new HashMap<>();
                map.put("orgId",resultSet.getObject("ORGID"));
                map.put("year",resultSet.getObject("YEAR"));
                map.put("total",resultSet.getObject("TOTAL"));
                map.put("visit",resultSet.getObject("VISIT"));
                list.add(map);
            }
        } catch (Exception e) {
            System.out.println("执行SQL异常!"+e);
        }finally {
            if(null !=pstmt){
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    System.out.println("关闭PreparedStatement异常!"+e.getMessage());
                }
            }
            try {
                if(conn != null && !conn.isClosed()){
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println("关闭连接异常!"+e.getMessage());
            }
        }
        return list;
    }

    public static int executeJdbc(String sql, Connection conn){
        int rows = 0;
        PreparedStatement pstmt = null;
        //1.创建自定义连接池对象
        try {
            //2.从池子中获取连接
            pstmt = conn.prepareStatement(sql);
            rows = pstmt.executeUpdate();
        } catch (Exception e) {
            System.out.println("执行SQL异常!"+e);
        }finally {
            if(null !=pstmt){
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    System.out.println("关闭PreparedStatement异常!"+e.getMessage());
                }
            }
            try {
                if(conn != null && !conn.isClosed()){
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println("关闭连接异常!"+e.getMessage());
            }
        }
        return rows;
    }

    public void validationQuery(Connection conn) {
        PreparedStatement pstmt = null;
        //1.创建自定义连接池对象
        try {
            String sql=" select 1 from dual";
            //2.从池子中获取连接
            pstmt = conn.prepareStatement(sql);
            boolean execute = pstmt.execute();
//            rows = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
