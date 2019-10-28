package Main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 17:22 2019/8/25
 */
public class HBaseUtil {


    /**
     * 初始化命名空间（相当于Mysql中的“库”）
     * @param conf
     * @param nameSpace
     */
    public static void initNameSpace(Configuration conf, String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        NamespaceDescriptor desc = NamespaceDescriptor.create(nameSpace)
                .addConfiguration("Author", "axin")
                .addConfiguration("TS", System.currentTimeMillis() + "")
                .build();


        admin.createNamespace(desc);
        close(admin,connection);
        System.out.println("命名空间创建完毕");



    }


    public static boolean isTableExist(Configuration conf,String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }


    /**
     * 创建表
     * @param conf
     * @param tableName
     * @param columnFamily 建议不要超过3个
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public static void createTable(Configuration conf,String tableName, String... columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //创建表属性对象,表名需要转字节
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //创建多个列族
        for(String cf : columnFamily){
            descriptor.addFamily(new HColumnDescriptor(cf));
        }
        //根据对表的配置，创建表
        admin.createTable(descriptor);
        System.out.println("表" + tableName + "创建成功！");
        close(admin,connection);
    }


    public static void dropTable(Configuration conf,String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if(isTableExist(conf,tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }


    public static void addRowData(Configuration conf,String tableName, String rowKey, String columnFamily, String column, String value) throws IOException{
        //创建HTable对象
        HTable hTable = new HTable(conf, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        //.add已过时，新API是：.addColumn
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }


    public static void deleteMultiRow(Configuration conf,String tableName, String... rows) throws IOException{
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }


    public static void getAllRows(Configuration conf,String tableName) throws IOException{
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }


    public static void getRow(Configuration conf,String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    public static void getRowQualifier(Configuration conf,String tableName, String rowKey, String family, String qualifier) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 关闭方法
     * @param admin
     * @param connection
     * @throws IOException
     */
    private static void close(HBaseAdmin admin, Connection connection) throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }

    }



    public static void main(String[] args) throws IOException {
        //使用HBaseConfiguration的单例方法实例化
        Configuration conf = HBaseConfiguration.create();
        String quorum = PropertiesUtil.getProperties("hbase.zookeeper.quorum");
        String port = PropertiesUtil.getProperties("hbase.zookeeper.property.clientPort");
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", port);

        initNameSpace(conf,"space2");
    }
}
