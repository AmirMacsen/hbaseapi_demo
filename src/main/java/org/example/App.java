package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    private static Connection connection;
    static {
        Configuration configuration = HBaseConfiguration.create();
        //设置Zookeeper集群
        configuration.set("hbase.zookeeper.quorum","node2,node3,node4");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间
     */
    public static void createNamespace(String namespace) throws IOException {
        // 获取admin对象，用于创建命名空间或者对表操作
        Admin admin = connection.getAdmin();
        // 创建命名空间描述器对象
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        // 创建命名空间
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }

    /**
     * 删除命名空间
     */
    public static void deleteNamespace(String namespace) throws IOException {
        // 获取admin对象，用于创建命名空间或者对表操作
        Admin admin = connection.getAdmin();
        // 删除命名空间
        admin.deleteNamespace(namespace);
        admin.close();
    }

    /**
     * 判断表是否存在
     */
    public static boolean tableExists(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            TableName table = TableName.valueOf(tableName);
            return admin.tableExists(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, String[] columnFamilies) throws IOException {
        try(Admin admin=connection.getAdmin()){
            TableName table = TableName.valueOf(tableName);
            // 判断表是否存在
            if (tableExists(tableName)) {
                System.out.println("表已存在");
                return;
            }
            // 列族有效性校验
            if (columnFamilies.length == 0) {
                System.out.println("列族不能为空");
                return;
            }
            // 创建表描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
            for (String columnFamily : columnFamilies) {
                // 创建列族描述器
                ColumnFamilyDescriptor columnFamilyDescriptor =
                        ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes()).build();
                // 将列族描述器添加到表描述器
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
        }
    }

    /**
     * 删除表
     */
    public static void deleteTable(String tableName) throws IOException {
        try(Admin admin=connection.getAdmin()){
            TableName table = TableName.valueOf(tableName);
            // 判断表是否存在
            if (!tableExists(tableName)) {
                System.out.println("表不存在");
                return;
            }
            // 删除表 删除之前先禁用
            admin.disableTable(table);
            admin.deleteTable(table);
        } catch (IOException e) {
            System.out.println("删除表失败");
        }

    }

    /**
     * 添加数据
     * @param           tableName 表名
     *                  rowKey 行键
     *                  columnFamily 列族
     *                  column 列
     *                  value 值
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        try(Table table=connection.getTable(TableName.valueOf(tableName))){
            Put put = new Put(rowKey.getBytes());
            put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
            table.put(put);
        } catch (IOException e) {
            System.out.println("添加数据失败");
        }
    }

    /**
     * 查询数据
     * @param tableName 表
     * @param rowKey
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey) throws IOException {
        try(Table table=connection.getTable(TableName.valueOf(tableName))){
            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }
    }

    /**
     * 扫描数据
     * @param tableName
     * @param startRowKey 起始rowKey 包含
     * @param stopRowKey 结束rowKey 不包含
     * @throws IOException
     */
    public static void scanData(String tableName,String startRowKey,String stopRowKey) throws IOException {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan(startRowKey.getBytes(), stopRowKey.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                }
            }
        }catch (IOException e){
            System.out.println("扫描数据失败");
        }
    }

    public static void deleteRow(String tableName, String rowKey) throws IOException {
        try(Table table=connection.getTable(TableName.valueOf(tableName))){
            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);
        }
    }

    /**
     * 删除数据列族下的指定列的数据
     * @throws IOException
     */
    public static void deleteData(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        try(Table table=connection.getTable(TableName.valueOf(tableName))){
            Delete delete = new Delete(rowKey.getBytes());
            delete.addColumn(columnFamily.getBytes(), column.getBytes());
            table.delete(delete);
        }
    }

    public static void main( String[] args ) throws IOException {
        System.out.println(connection);
        // 创建命名空间
        try {
            createNamespace("test1");
        } catch (IOException e) {
            System.out.println("创建hbase namespace失败");
        }

        try {
            deleteNamespace("test1");
        } catch (IOException e) {
            System.out.println("删除hbase namespace失败");
        }

        boolean test1 = tableExists("test1");
        System.out.println("表存在：" + test1);

        // 创建表
        try {
            String[] columnFamilies = {"cf1", "cf2"};
            createTable("test1", columnFamilies);
        } catch (IOException e) {
            System.out.println("创建表失败");
        }

        try{
            // deleteTable("test1");
        }catch (Exception e){
            System.out.println("删除表失败");
        }

        try {
            putData("test1", "row1", "cf1", "name", "zhangsan");
            putData("test1", "row2", "cf2", "age", "18");
            putData("test1", "row2", "cf2", "gender", "男");
        } catch (IOException e) {
            System.out.println("添加数据失败" + e.getMessage());
        }

        try {
            getData("test1", "row1");
        } catch (IOException e) {
            System.out.println("查询数据失败");
        }

        try {
            scanData("test1", "row1", "row3");
        }catch (IOException e){
            System.out.println("扫描数据失败");
        }

        try {
            deleteData("test1", "row2", "cf2", "age");
        } catch (IOException e) {
            System.out.println("删除数据失败");
        }

        try {
            deleteRow("test1", "row2");
        } catch (IOException e) {
            System.out.println("删除数据失败");
        }
    }
}
