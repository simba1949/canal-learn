package vip.openpark.ha.connect;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * @author anthony
 * @version 2024-08-30
 * @since 2024-08-30 10:19
 */
public class HaConnectApplication {
	public static void main(String[] args) {
		listenStart();
	}

	/**
	 * 监听数据
	 */
	public static void listenStart() {
		String zkServer = "192.168.0.110:2181";
		// destination 表示的是 canal server 的实例，默认的实例名称是 example，它代表一个完整的监听实例。
		String destination = "example";
		// 创建 canal 连接
		CanalConnector canalConnector = CanalConnectors.newClusterConnector(zkServer, destination, "", "");

		int batchSize = 100;

		canalConnector.connect();
		canalConnector.subscribe(".*\\..*");
		canalConnector.rollback();

		while (true) {
			// 获取指定数量的数据
			Message message = canalConnector.getWithoutAck(batchSize);
			long batchId = message.getId();
			int size = message.getEntries().size();
			if (batchId == -1 || size == 0) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					//
				}
			} else {
				System.out.println("batchId:" + batchId + ",size:" + size);
				printEntry(message.getEntries());
			}
		}
	}

	/**
	 * 打印数据
	 *
	 * @param entries 数据列表
	 */
	public static void printEntry(List<CanalEntry.Entry> entries) {
		for (CanalEntry.Entry entry : entries) {
			if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
				    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
				continue;
			}

			CanalEntry.Header header = entry.getHeader();
			// 获取数据库名
			String schemaName = header.getSchemaName();
			// 获取表名
			String tableName = header.getTableName();
			// 获取操作类型
			CanalEntry.EventType headEventType = header.getEventType();
			long executeTime = header.getExecuteTime();
			LocalDateTime changeDateTime = LocalDateTime.ofEpochSecond(executeTime / 1000, 0, ZoneOffset.ofHours(8));
			System.out.printf("table[%s.%s], eventType : %s , changeDateTime : %s%n", schemaName, tableName, headEventType, changeDateTime);

			CanalEntry.RowChange rowChange = null;
			try {
				rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
			} catch (Exception e) {
				System.out.println("ERROR ## parser of eromanga-event has an error , data:" + entry.toString());
				continue;
			}

			CanalEntry.EventType eventType = rowChange.getEventType();
			for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
				if (eventType == CanalEntry.EventType.DELETE) {
					printColumn(schemaName, tableName, rowData.getBeforeColumnsList());
				} else if (eventType == CanalEntry.EventType.INSERT) {
					printColumn(schemaName, tableName, rowData.getAfterColumnsList());
				} else {
					System.out.println("-------&gt; before");
					printColumn(schemaName, tableName, rowData.getBeforeColumnsList());
					System.out.println("-------&gt; after");
					printColumn(schemaName, tableName, rowData.getAfterColumnsList());
				}
			}
		}
	}

	/**
	 * 打印数据
	 *
	 * @param schemaName  数据库名
	 * @param tableName   表名
	 * @param columnsList 字段列表
	 */
	private static void printColumn(String schemaName, String tableName, List<CanalEntry.Column> columnsList) {
		for (CanalEntry.Column column : columnsList) {
			System.out.printf("table[%s.%s], column[%s], value[%s]%n", schemaName, tableName, column.getName(), column.getValue());
		}
	}
}