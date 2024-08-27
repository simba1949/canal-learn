package vip.openpark.kafka.listener;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.protocol.FlatMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author anthony
 * @version 2024-08-27
 * @since 2024-08-27 21:47
 */
@Slf4j
@Component
public class CanalToKafkaListener {

	@KafkaListener(topics = "example")
	public void handle(ConsumerRecord<String, String> consumerRecord) {
		log.info("receive message:{}", consumerRecord);

		// 使用 fastjson2（ canal.client 会自动依赖进来）反序列化数据库变动日志 FlatMessage
		FlatMessage flatMessage = JSON.parseObject(consumerRecord.value(), FlatMessage.class);
		log.info("flatMessage is:{}", flatMessage);

		log.info("table{}.{}，变动类型：{}", flatMessage.getDatabase(), flatMessage.getTable(), flatMessage.getType());
		// 变动前后对象类型是：List<Map<String, String>>
		log.info("变动前数据：{}，变动后数据：{}", flatMessage.getOld(), flatMessage.getData());
	}
}