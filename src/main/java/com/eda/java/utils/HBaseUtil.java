package com.eda.java.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseUtil {
    private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>();
    private static Connection conn = null;

    public static Connection makeConnection() throws Exception {
        Connection connection = connectionThreadLocal.get();

        if (connection == null) {
            Configuration configuration = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(configuration);
            connectionThreadLocal.set(conn);
        }

        return connection;
    }

    public static boolean isTableExist(String tableName) throws Exception {
        Admin admin = conn.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName, List<String> columnFamilies) throws Exception {
        Admin admin = conn.getAdmin();

        if (!isTableExist(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cf : columnFamilies) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            admin.createTable(descriptor);
        }
    }

    /**
     * 创建分区和分区键
     * */
    public static void createTable(String tableName, int regionCount, List<String> columnFamilies) throws Exception {
        Admin admin = conn.getAdmin();

        if (!isTableExist(tableName)) {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cf : columnFamilies) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            byte[][] bs = genRegionKeys(regionCount);

            admin.createTable(descriptor, bs);
        }
    }

    /**
     * 生成分区号前缀的row key
     * */
    public static String getRegionNumRowkey(String rowKey, int regionCount) {
        int reginNum;
        int hash = rowKey.hashCode() & Integer.MAX_VALUE;//hashcode可能为负数，确保其为正数

        //hash散列平均分配
        if (regionCount > 0 && (regionCount & (regionCount -1)) == 0) {
            reginNum = hash & (regionCount - 1);
        } else {
            reginNum = hash % regionCount;
        }

        System.out.println(reginNum);

        return reginNum + "_" + rowKey;
    }

    /**
     * 生成分区键
     * */
    public static byte[][] genRegionKeys(int regionCount) {
        byte[][] bs = new byte[regionCount - 1][];

        for (int i = 0; i < regionCount - 1; i++) {
            String regionKey = i + "|";
            bs[i] = Bytes.toBytes(regionKey);
            System.out.println("region key is: " + regionKey);
        }

        return bs;
    }

    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);
        table.close();
    }

    public static void deleteTable(String tableName) throws Exception {
        Admin admin = conn.getAdmin();

        if (isTableExist(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
    }

    public static void close() throws Exception {
        Connection connection = connectionThreadLocal.get();
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws Exception {
        makeConnection();



        close();
    }
}
