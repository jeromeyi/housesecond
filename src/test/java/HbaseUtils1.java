import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import static org.apache.hadoop.hbase.client.Durability.SYNC_WAL;


public class HbaseUtils1 {
    public static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    public static final String ZK_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    private Configuration conf = HBaseConfiguration.create();
    private Connection connection;
    private Admin admin;
    private String HBASEENCODE = "utf8";// 编码格式

    // 构造函数
    public HbaseUtils1() {

    }

    // 获取配置环境
    public Configuration getConf() {
        return conf;
    }

    /*
     * 一般情况下，需要在conf里面加入配置文件，有地址，还有端口。 conf = HBaseConfiguration.create();
     * conf.set("hbase.zookeeper.quorum", "192.168.6.91");
     * conf.set("hbase.zookeeper.property.clientPort", "2181");
     */

    /**
     * 初始化，参数是配置文件的路径，初始化之后admin就可以使用了
     *
     * @param configFile
     *            是配置文件的路径，配置文件就是集群Linux目录里面 HBase文件夹下conf里面的hbase-site.xml
     *            将这个文件拷贝到自己的电脑里面，然后参数就是这个文件的路径，打包的时候将这个文件一起打包，所以最好是直接放在这个工程的文件夹下
     *            这个方法是适合各种变化的环境，只要复制hbase-site.xml就可以在任意集群上跑。当然如果集群是不变的，也可以按照上面的写法给写死了
     * @throws IOException
     */
    public void init(String configFile) throws IOException {
        conf.addResource(new Path(configFile));
        // Connection 创建是个重量级的工作，线程安全，是操作hbase的入口
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    public void close() {
        try {
            if (admin != null)
                admin.close();
            if (connection != null)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个表
     *
     * @param table_name
     *            表名称
     * @param family_names
     *            列族名称集合
     * @throws IOException
     */
    public void create_table(String table_name, String[] family_names) throws IOException {
        if (!tableExist(table_name)) {
            // 获取TableName
            TableName tableName = TableName.valueOf(table_name);
            // table 描述
            HTableDescriptor htabledes = new HTableDescriptor(tableName);
            for (String family_name : family_names) {
                // column 描述
                HColumnDescriptor family = new HColumnDescriptor(family_name);
                htabledes.addFamily(family);
            }
            admin.createTable(htabledes);
        }
    }

    /**
     * 增加一行的记录
     *
     * @param table_name
     *            表名称
     * @param row
     *            行主键，用|拼接
     * @param columnFamily
     *            列族名称
     * @param column
     *            列名(可以为null)
     * @param value
     *            值
     * @param num
     *            数组的长度和columnFamily一样，每个元素代表对应的columnFamily有几个column
     * @throws IOException
     */
    public void addData_One(String table_name, String row, String[] columnFamily, String[] column, String[] value,
                            int[] num) throws IOException {
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        // put对象 负责录入数据
        Put put = new Put(row.getBytes(this.HBASEENCODE));// 指定行
        int count = 0;
        for (int i = 0; i < columnFamily.length; i++) {
            for (int j = count; j < count + num[i]; j++) {
                put.addColumn(columnFamily[i].getBytes(this.HBASEENCODE), column[j].getBytes(this.HBASEENCODE),
                        value[j].getBytes(this.HBASEENCODE));
            }
            count = count + num[i];
        }
        /*
         * 设置写WAL（Write-Ahead-Log）的级别 参数是一个枚举值，可以有以下几种选择： ASYNC_WAL ：
         * 当数据变动时，异步写WAL日志，数据量巨大时，有可能丢失 SYNC_WAL ： 当数据变动时，同步写WAL日志,确保不会丢失
         * FSYNC_WAL ： 当数据变动时，同步写WAL日志，并且，强制将数据写入磁盘 SKIP_WAL ： 不写WAL日志
         * USE_DEFAULT ： 使用HBase全局默认的WAL写入级别，即SYNC_WAL
         */
        put.setDurability(SYNC_WAL);
        table.put(put);
        table.close();
    }

    /**
     * 批量插入
     *
     * @param table_name
     *            表名
     * @param row
     *            行主键
     * @param columnFamily
     *            列簇，表中共有几个列簇
     * @param column
     *            列名，表中共有几个列
     * @param value
     *            值，这个数组的长度=row的长度*column的长度，顺序是按照行从左到右依次填入
     * @param num
     *            数组的长度和columnFamily一样，每个元素代表对应的columnFamily有几个column
     * @throws IOException
     */
    public void addData_Multi(String table_name, String[] row, String[] columnFamily, String[] column, String[] value,
                              int[] num) throws IOException {
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        ArrayList<Put> list = new ArrayList<Put>();
        int count2 = 0;// 记录value的下标
        for (int m = 0; m < row.length; m++) {
            Put put = new Put(row[m].getBytes(this.HBASEENCODE));// 指定行
            int count1 = 0;// 定位column的位置
            for (int i = 0; i < columnFamily.length; i++) {
                for (int j = count1; j < count1 + num[i]; j++) {
                    put.addColumn(columnFamily[i].getBytes(this.HBASEENCODE), column[j].getBytes(this.HBASEENCODE),
                            value[count2].getBytes(this.HBASEENCODE));
                    count2++;
                }
                count1 = count1 + num[i];
            }
            put.setDurability(SYNC_WAL);
            list.add(put);
        }
        table.put(list);
        table.close();
    }

    /**
     * 判断表是否存在
     *
     * @param table_name
     * @return
     * @throws IOException
     */
    public boolean tableExist(String table_name) throws IOException {
        return admin.tableExists(TableName.valueOf(table_name));
    }

    /**
     * 删除表
     *
     * @param table_name
     * @throws IOException
     */
    public void deleteTable(String table_name) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    /**
     * 删除一条记录
     *
     * @param table_name
     *            表名
     * @param row
     *            行主键
     * @throws IOException
     */
    public void delete_row(String table_name, String row) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        Delete del = new Delete(row.getBytes(this.HBASEENCODE));
        table.delete(del);
    }

    /**
     * 删除多条记录
     *
     * @param table_name
     *            表名
     * @param rows
     *            需要删除的行主键
     * @throws IOException
     */
    public void delMultiRows(String table_name, String[] rows) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        ArrayList<Delete> delList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete del = new Delete(row.getBytes(this.HBASEENCODE));
            delList.add(del);
        }
        table.delete(delList);
    }

    /**
     * 查询单个row的记录
     *
     * @param table_name
     *            表名
     * @param row
     *            行键
     * @param columnfamily
     *            列族，可以为null
     * @param column
     *            列名，可以为null
     * @return
     * @throws IOException
     */
    public Cell[] getRow(String table_name, String row, String columnfamily, String column) throws IOException {

        // table_name和row不能为空
        if (StringUtils.isEmpty(table_name) || StringUtils.isEmpty(row)) {
            return null;
        }
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Get get = new Get(row.getBytes(this.HBASEENCODE));
        // 判断在查询记录时,是否限定列族和列名
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)) {
            get.addColumn(columnfamily.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isEmpty(column)) {
            get.addFamily(columnfamily.getBytes(this.HBASEENCODE));
        }

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        return cells;
    }

    /**
     * 获取表中的部分记录,可以指定列族,列族成员,开始行键,结束行键.
     *
     * @param table_name
     *            表名
     * @param family
     *            列簇
     * @param column
     *            列名
     * @param startRow
     *            开始行主键
     * @param stopRow
     *            结束行主键,结果不包含这行，到它前面一行结束
     * @return
     * @throws Exception
     */
    public ResultScanner scan_part_Table(String table_name, String family, String column, String startRow,
                                         String stopRow) throws IOException {
        // Table
        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column)) {
            scan.addColumn(family.getBytes(this.HBASEENCODE), column.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(family) && StringUtils.isEmpty(column)) {
            scan.addFamily(family.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(startRow)) {
            scan.setStartRow(startRow.getBytes(this.HBASEENCODE));
        }
        if (StringUtils.isNotEmpty(stopRow)) {
            scan.setStopRow(stopRow.getBytes(this.HBASEENCODE));
        }
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }

    /**
     * 根据过滤器进行查找
     *
     * @param table_name
     *            表名
     * @param filter
     *            过滤器，就是过滤条件
     * @return
     * @throws IOException
     */
    public ResultScanner scan_filter_Table(String table_name, Filter filter) throws IOException {
        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }

    /**
     * 获取表中的全部记录
     *
     * @param table_name
     *            表名
     * @return
     * @throws IOException
     */
    public ResultScanner scanTableAllData(String table_name) throws IOException {
        Table table = connection.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;

    }

    /**
     * 获得HBase里面所有的表名
     *
     * @return
     * @throws IOException
     */
    public List<String> getAllTables() throws IOException {
        List<String> tables = new ArrayList<String>();
        if (admin != null) {
            HTableDescriptor[] allTable = admin.listTables();
            if (allTable.length > 0) {
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                }
            }
        }
        return tables;
    }

    /**
     * 获得表的描述
     *
     * @param tableName
     * @return
     */
    public String describeTable(String tableName) {
        try {
            return admin.getTableDescriptor(TableName.valueOf(tableName)).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 打印多条记录
     * <p>
     * ResultScanner把扫描操作转换为类似的get操作，它将每一行数据封装成一个Result实例，并将所有的Result实例放入一个迭代器中。
     * 用完ResultScanner 后一定要记得关掉：resultScanner.close()
     * rawCells()返回这一行的所有单元格，每个单元格由行主键，列簇，列名，时间戳，值组成
     *
     * @param resultScanner
     */
    public void printallRecoder(ResultScanner resultScanner) {
        for (Iterator<Result> it = resultScanner.iterator(); it.hasNext();) {
            Result result = it.next();
            Cell[] cells = result.rawCells();
            printRecoder(cells);
        }
    }

    /**
     * 打印一条记录
     *
     * @param cells
     */
    public void printRecoder(Cell[] cells) {
        for (Cell cell : cells) {
            // 一个Cell就是一个单元格，通过下面的方法可以获得这个单元格的各个值
            System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
            System.out.print("列簇: " + new String(CellUtil.cloneFamily(cell)));
            System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
            System.out.println("时间戳: " + cell.getTimestamp());
        }
    }
}
