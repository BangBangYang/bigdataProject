package com.bupt.test;

import com.bupt.constants.Constants;
import com.bupt.dao.HBaseDao;
import com.bupt.utils.HBaseUtil;

import java.io.IOException;

public class WeiBoTest {
    public static void init(){

        try {
            //创建命名空间
            HBaseUtil.createNameSpace(Constants.NAMESPACE);
            //创建微博内容表
            HBaseUtil.createTable(Constants.CONTENT_TABLE,Constants.CONTENT_TABLE_VERSIONS,Constants.CONTENT_TABLE_CF);
            //创建用户关系表
            HBaseUtil.createTable(Constants.RELATION_TABLE,Constants.RELATION_TABLE_VERSIONS,Constants.RELATION_TABLE_CF1,Constants.RELATION_TABLE_CF2);
            //创建收件箱表
            HBaseUtil.createTable(Constants.INBOX_TABLE,Constants.INBOX_TABLE_VERSIONS,Constants.INBOX_TABLE_CF);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) throws IOException {
        //初始化
//        init();
        //1001 发布微博
//        HBaseDao.publishWeiBo("1001","下雨了啊!!!");
        //1002 关注1001和1003
//        HBaseDao.addAttends("1002","1001","1003");
        //获取1002初始化页面
        HBaseDao.getInit("1002");
        //1003 发布三条微博，同时1001发布两条微博
//        HBaseDao.publishWeiBo("1003","a下雨了啊!!!");
//        HBaseDao.publishWeiBo("1003","b下雨了啊!!!");
//        HBaseDao.publishWeiBo("1003","c下雨了啊!!!");
//        HBaseDao.publishWeiBo("1001","1001+下雨了啊!!!");
//        HBaseDao.publishWeiBo("1001","1001++下雨了啊!!!");
        //获取1002初始化页面
        System.out.println("-------------------22222----------------------");
//        HBaseDao.getInit("1002");
        //1002取关1003
//        HBaseDao.deleteAttends("1002","1003");
        //获取1002初始化页面
        System.out.println("-------------------333-----------------------");
//        HBaseDao.getInit("1002");
        //1002再次关注1003
 //       HBaseDao.addAttends("1002","1003");
        //获取1002初始化页面
        System.out.println("-------------------44-----------------------");
//        HBaseDao.getInit("1002");
        //获取1001微博详情
        System.out.println("-------------------55-----------------------");
        HBaseDao.getWeibo("1001");
    }
}
