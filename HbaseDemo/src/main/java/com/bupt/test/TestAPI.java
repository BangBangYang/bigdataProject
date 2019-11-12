package com.bupt.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * DDL:
 * 1. 创建命名空间
 * 2. 创建表
 * 3. 创建命名表
 * 4. 删除表
 *
 * DML：
 * 5. 插入数据
 * 6. 查数据（get）
 * 7. 查数据（scan）
 * 8. 删除数据
 *
 */
public class TestAPI {

    //方式一：旧api 1. 判断表是否存在
    public static boolean isTableExisit(String tableName) throws IOException {
       /*
       过时的api
       //1. 获取配置文件信息
        HBaseConfiguration configuration = new HBaseConfiguration();
        //2. 获取管理员对象
        configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop101:2181");

        HBaseAdmin admin = new HBaseAdmin(configuration);
        //3. 判断表对象是否存在
        boolean exists = admin.tableExists(tableName);
        //4. 关闭连接
        admin.close();
        //5 返回对象
        return exists;
     */

        //方式二：新api 1. 判断表是否存在
        //1. 获取配置文件信息
        Configuration configuration = HBaseConfiguration.create();
      //  configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop101:2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.1.101,192.168.1.102,192.168.1.103");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //2. 获取管理员对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //3. 判断表对象是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //4. 关闭连接
        admin.close();
        //5 返回对象
        return exists;

    }

    public static void main(String[] args) throws IOException {
       //1. 测试表是否存在
        System.out.println(isTableExisit("student"));
    }
}
