package com.bupt.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
/*
创建三张表
判断表是否存在
创建命名空间
 */
import static com.bupt.constants.Constants.CONFIGURATION;

public class HBaseUtil {
    public static void createNameSpace(String nameSpace) throws IOException {

            Connection  connection= null;
            Admin admin = null;
         //1 获取Connection对象
       // CONFIGURATION.set("hbase.zookeeper.quorum", "192.168.1.101,192.168.1.102,192.168.1.103");
      //  CONFIGURATION.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(CONFIGURATION);
         //2 获得admin对象
            admin= connection.getAdmin();
         //3 构建空间描述器
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
         //4 创建命名空间
            admin.createNamespace(namespaceDescriptor);
         //5 关闭资源
            admin.close();
            connection.close();

    }
    //2 判断表是否存在
    private static boolean isTableExisit(String tableName) throws IOException {
        CONFIGURATION.set("hbase.zookeeper.quorum", "192.168.1.101,192.168.1.102,192.168.1.103");
        CONFIGURATION.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(CONFIGURATION);
        Admin admin = connection.getAdmin();
        //判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //关闭资源
        admin.close();
        connection.close();
        //返回结果
        return exists;
    }
    //3  创建表
    public static void createTable(String tableName, int versions, String...cfs) throws IOException {
        //1 判断是否传入列族信息
        if(cfs.length <= 0){
            System.out.println("请设置列族信息！！！");
            return;
        }
        //2 判断表是否存在
        if(isTableExisit(tableName)){
            System.out.println(tableName+"表已存在！！！");
            return;
        }
        //2 构建connection对象
        CONFIGURATION.set("hbase.zookeeper.quorum", "192.168.1.101,192.168.1.102,192.168.1.103");
        CONFIGURATION.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(CONFIGURATION);

        //4 获取admin对象
        Admin admin = connection.getAdmin();
        //5 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //6 添加列族信息
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //7 设置版本
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //8 创建表操作
        admin.createTable(hTableDescriptor);
        //9 关闭资源
        admin.close();
        connection.close();
    }
}
