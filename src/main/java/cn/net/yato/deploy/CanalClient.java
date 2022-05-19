package cn.net.yato.deploy;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.protocol.CanalEntry.*;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author CaoWei
 * @ClassName: CanalClient
 * @Description: TODO(canal客户端)
 * @date 2021/11/30
 */
@Component
public class CanalClient implements InitializingBean {

    // 获取数据的数量
    private final static int BATCH_SIZE = 1000;

    /**
     * @Title: afterPropertiesSet
     * @Description: TODO(使用Spring Bean的生命周期函数)
     * @param 
     * @return  返回类型
     * @author CaoWei
     * @date 2021/11/30
     * @throws 
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 创建连接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1",11111),"example","","");
        try {
            // 打开连接
            connector.connect();
            // 订阅数据库表，全部表
            connector.subscribe(".*\\..*");
            // 回滚到未进行ack的地方，下次retch的时候，可以从最后一个没有ack的地方开始拿
            connector.rollback();
            while (true){
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(BATCH_SIZE);
                // 获取批量ID
                long id = message.getId();
                // 获取批量的数量
                int size = message.getEntries().size();
                // 如果没有数据
                if (id == -1 || size == 0){
                    try {
                        // 线程休眠两秒
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else {
                    // 如果有数据则处理数据
                    printEntry(message.getEntries());
                }
                // 进行 batch id的确认，确认之后，小于等于此 batchid的Message都会被确认
                connector.ack(id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }
    }

    /**
     * @Title: printEntry
     * @Description: TODO(打印canal server解析binlog获得的实体类信息)
     * @param entrys
     * @return  返回类型
     * @author CaoWei
     * @date 2021/11/30
     * @throws
     */
    private static void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                // 开启/关闭事务的实体类型，跳过
                continue;
            }
            // RowChange对象，包含了一行数据变化的所有特征
            // 比如isDdl 是否是ddl变更操作 sql 具体的ddl sql beforeColumns afterColumns 变更前后的数据字段等等
            RowChange rowChage;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }
            // 获取操作类型：insert/update/delete类型
            EventType eventType = rowChage.getEventType();
            // 打印Header信息
            System.out.println(String.format("================》; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            // 判断是否是DDL语句
            if (rowChage.getIsDdl()) {
                System.out.println("================》;isDdl: true,sql:" + rowChage.getSql());
            }
            // 获取RowChange对象里的每一行数据，打印出来
            for (RowData rowData : rowChage.getRowDatasList()) {
                // 如果是删除语句
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                    // 如果是新增语句
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                    // 如果是更新的语句
                } else {
                    // 变更前的数据
                    System.out.println("------->; before");
                    printColumn(rowData.getBeforeColumnsList());
                    // 变更后的数据
                    System.out.println("------->; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    /**
     * @Title: printColumn
     * @Description: TODO(打印信息)
     * @param columns
     * @return  返回类型
     * @author CaoWei
     * @date 2021/11/30
     * @throws
     */
    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
