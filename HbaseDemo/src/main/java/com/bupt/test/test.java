package com.bupt.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class test {
    private static Connection connection = null;
    private static Admin admin = null;
    static {
        Configuration configuration = HBaseConfiguration.create();
        //configuration.set("hbase.zookeeper.quorum", "hadoop102:2181,hadoop103:2181");
       // configuration.set("hbase.zookeeper.quorum", "192.168.1.101,192.168.1.102,192.168.1.103");
       // configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum",":hdp2.buptnsrc.com:2181,hdp4.buptnsrc.com:2181,hdp1.buptnsrc.com:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void close() throws IOException {
        if(admin != null){
            admin.close();
        }
        if(connection != null){
            connection.close();
        }
    }
    public static boolean isTableExisit(String tableName) throws IOException {
        //方式三： 静态代码块
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }
    //2. 创建表
    public static void creatTable(String tableName, String... cfs) throws IOException {
        //1. 判断是否存在列族信息
        if(cfs.length <= 0){
            System.out.println("NO!!!");
            return;
        }
        //2. 判断表已存在
        if(isTableExisit(tableName)){
            System.out.println(tableName+"表已存在！！！");
            return;
        }
        //3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //4. 循环添加具体列族信息
        for (String cf : cfs) {
            //5 创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //6  循环添加具体列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //7 创建表
        admin.createTable( hTableDescriptor);
    }
    //3 删除表
    public static void droptable(String tableName) throws IOException {
        //1 判断表是否存在
        if(!isTableExisit(tableName)){
            System.out.println(tableName+"表不存在！！！");
        }
        //2 使表下线
        admin.disableTable(TableName.valueOf(tableName));
        //3 删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }
    //4 创建命名空间
    public static void createNameSpace(String ns){
        //1 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        //2 创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println("命名空间已存在");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        //
    }
    //5 向表中插入数据
    public static void putData(String tableName,String rowKey,String cf, String cn,String value) throws IOException {
        //1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //3 put对象赋值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //4 插入数据
        table.put(put);
        //5 关闭资源
        table.close();
    }
    //6 获取数据
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2 创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
            //2.1 指定获取列族
//        get.addFamily(Bytes.toBytes(cf));
            //2.2 指定获取列族和列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //3 获取数据
        Result result = table.get(get);
        //4 解析result打印
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("CF: "+Bytes.toString(CellUtil.cloneFamily(cell))+
            " CN: "+Bytes.toString(CellUtil.cloneQualifier(cell))+
            " Value: "+Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
        //5 关闭表连接
        table.close();
    }
    //7 扫描全表数据
    public static void scanTable(String tableName) throws IOException {
       //1 构建表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2 构建scan对象
        //Scan scan = new Scan();
        //2.1 过滤的scan对象
        Scan scan = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1002"));//左闭右开
        //3 扫描表
        ResultScanner resultScanner = table.getScanner(scan);
        //4 解析resultScanner
        for (Result result:resultScanner) {
            //5 解析result并打印
            for (Cell cell : result.rawCells()) {
                //6 打印数据
                System.out.println("CK: "+Bytes.toString(CellUtil.cloneRow(cell))+"CF: "+Bytes.toString(CellUtil.cloneFamily(cell))+
                        " CN: "+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        " Value: "+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //7 关闭资源
        table.close();
    }
    //8 删除数据
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2 构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
            //2.1 设置删除的列
        //delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
            //2.2 删除指定列族
        delete.addFamily(Bytes.toBytes(cf));
        //3 执行删除操作
        table.delete(delete);
        //4 关闭连接
        table.close();
    }
    public static void main(String[] args) throws IOException {
       // System.out.println(admin.tableExists(TableName.valueOf("student")));
        //1 判断表是否存在
 //       System.out.println(isTableExisit("stu2"));
        //2 创建表测试
//       creatTable("namespaceTest:stu2","info1","info2");

        //3 删除表测试
 //       droptable("stu2");
 //       System.out.println(isTableExisit("stu2"));

        //4 创建命名空间
        createNameSpace("yk_namespace");
        //5 插入数据
//        putData("student","1002","info","name","ll");
      //6 取单行数据
//        getData("student","1001","info","name");
        //7 scan扫描全表
//        scanTable("student");
        //8 测试删除数据
//        deleteData("stu2","1002","info2","name");
        //关闭资源
        System.out.println(admin.toString());
        close();
    }
}
