package org.example.calllog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * 10个用户10年的通话记录分析
 */
public class CallLogDemo {
    // 时间格式化对象
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 命名空间
    private static final String NAMESPACE = "calllog";
    // 表名
    private static final String TABLE_NAME = "calllog";
    // 表名对应的tableName
    private static TableName tableName;
    // 表DDL对象
    private static Admin admin;
    // 表数据对象 mdl
    private static Table table;

    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node2,node3,node4");

        try {
            // 连接对象
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 环境初始化
     * @throws IOException
     */
    public static void init() throws IOException {
       admin=connection.getAdmin();
       createNamespace();
       tableName=TableName.valueOf(NAMESPACE + ":" + TABLE_NAME);
       table=connection.getTable(tableName);
    }

    /**
     * 资源关闭
     */
    public static void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 创建命名空间
     */
    public static void createNamespace() throws IOException {
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
            if (NAMESPACE.equals(namespaceDescriptor.getName())) {
                System.out.println("命名空间已存在");
                return;
            }else {
                NamespaceDescriptor newNamespaceDescriptor = NamespaceDescriptor.create(NAMESPACE).build();
                admin.createNamespace(newNamespaceDescriptor);
                System.out.println("创建命名空间成功");
            }
        }
    }

    /**
     * 创建表
     */
    public static void createTable(String[]columnFamilies) throws IOException {
        String tableFullName = NAMESPACE + ":" + TABLE_NAME;
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableFullName));
        // 列族描述器
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes());
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        // 如果存在先禁用后删除
        if (admin.tableExists(TableName.valueOf(tableFullName))){
            admin.disableTable(tableDescriptor.getTableName());
            admin.deleteTable(tableDescriptor.getTableName());
        }

        System.out.println("进行到这一步了");
        // 创建表
        admin.createTable(tableDescriptorBuilder.build());
    }

    /**
     * 生成10个用户的10000条通话记录
     * dnum 手机号码 type 呼叫类型 0 呼出 1呼入  length 通话时长 date 通话时间
     */
    public static void insertData() throws Exception {
        Random random = new Random();
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // 清理表格
            puts.clear();
            // 生成rowkey 手机号码
            String phoneNumber = getPhoneNumber("186");
            System.out.println("rowkey:" + phoneNumber);
            for (int j = 0; j < 10000; j++) {
                String dnum = getPhoneNumber("199");
                int length = random.nextInt(200) + 1;
                int type = random.nextInt(2);
                String date = getDate(2024);
                // rowKey的设计
                String rowkey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime() + i +j);
                Put put = new Put(rowkey.getBytes());
                put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("dnum"), Bytes.toBytes(dnum));
                put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("type"), Bytes.toBytes(type));
                put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("length"), Bytes.toBytes(length));
                put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("date"), Bytes.toBytes(date));
                puts.add(put);
            }
            // 提交数据
            table.put(puts);
        }
    }

    /**
     * 生成2024年的日期时间
     * @return 时间字符串
     */
    private static String getDate(int year) {
        Random random = new Random();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2024, Calendar.JANUARY, 1);
        calendar.add(Calendar.MONTH, random.nextInt(12));
        calendar.add(Calendar.DAY_OF_MONTH, random.nextInt(30));
        calendar.add(Calendar.HOUR_OF_DAY, random.nextInt(24));
        return sdf.format(calendar.getTime());
    }

    /**
     * 生成手机号码
     * @param prefix 前缀
     * @return 11位的手机号码
     */
    private static String getPhoneNumber(String prefix) {
        // 生成8位数字号码
        StringBuilder prefixBuilder = new StringBuilder(prefix);
        for (int i = 0; i < 8; i++) {
            prefixBuilder.append((int) (Math.random() * 10));
        }
        return prefixBuilder.toString();
    }

    /**
     * 获取数据 查询某用户3月的通话记录
     * rowkey:18661990012
     * rowkey:18636815365
     * rowkey:18602411838
     * rowkey:18644801771
     * rowkey:18649067703
     * rowkey:18667217456
     * rowkey:18631327851
     * rowkey:18692920608
     * rowkey:18604680121
     * rowkey:18632398272
     */
    public static void scanData() throws Exception {
        String phoneNumber="18661990012";
        // 这里数据存储的时候会排序，Long.MAX_VALUE - 时间戳 越小的 越靠前
        String startRow = phoneNumber+"_"+(Long.MAX_VALUE-
                sdf.parse("2024-04-01 00:00:00").getTime());
        //3.定义stopRow 包含
        String stopRow = phoneNumber+"_"+(Long.MAX_VALUE-
                sdf.parse("2024-03-01 00:00:00").getTime());
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow), true);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result:resultScanner){
            Cell[] cells = result.rawCells();
            String rowInfo = "rowkey:"+Bytes.toString(CellUtil.cloneRow(cells[0]));
            rowInfo +=","+Bytes.toString(CellUtil.cloneQualifier(cells[0]))
                    +":"+Bytes.toString(CellUtil.cloneValue(cells[0]));
            rowInfo +=","+Bytes.toString(CellUtil.cloneQualifier(cells[1]))
                    +":"+Bytes.toString(CellUtil.cloneValue(cells[1]));
            rowInfo +=","+Bytes.toString(CellUtil.cloneQualifier(cells[2]))
                    +":"+Bytes.toInt(CellUtil.cloneValue(cells[2]));
            rowInfo +=","+Bytes.toString(CellUtil.cloneQualifier(cells[3]))
                    +":"+Bytes.toInt(CellUtil.cloneValue(cells[3]));
            System.out.println(rowInfo);
        }
    }

    /**
     * 删除指定某行的某列 某单元格
     */
    public static void deleteRowCell() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("18661990012_9223370324984787969"));
        delete.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("length"));
        table.delete(delete);
    }

    /**
     * 添加指定某行某列 某单元格
     */
    public static void insertRowCell() throws IOException {
        Put put = new Put(Bytes.toBytes("18661990012_9223370324984787969"));
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("length"), Bytes.toBytes(100));
        table.put(put);
    }
    public static void main(String[] args) throws IOException {
        try {
            CallLogDemo.init();
//            String[] columnFamilies = {"basic"};
//            CallLogDemo.createTable(columnFamilies);
//            CallLogDemo.insertData();
            scanData();
//            deleteRowCell();
//            insertRowCell();
        }  catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            CallLogDemo.close();
        }

    }

}
