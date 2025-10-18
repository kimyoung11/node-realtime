const { Kafka } = require('kafkajs');

// Docker Compose 환경 변수에서 Kafka Broker 주소를 가져옴.
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC_NAME = 'analytics_events';

const kafka = new Kafka({
  clientId: 'analytics-producer',
  brokers: [KAFKA_BROKER], 
  // KafkaJS 라이브러리 레벨
  // 이 재시도가 모두 실패했을 때 앱 레벨에서 다시 시도하도록 로직을 수정.
  retry: {
      initialRetryTime: 100,
      retries: 5 
  }
});

const producer = kafka.producer();

const produceMessage = async () => {
    try {
        //임시 카프카 로그 생성용
        const message = {
            id: Date.now(),
            user_id: `user-${Math.floor(Math.random() * 100)}`, 
            event_type: ['PAGE_VIEW', 'CLICK', 'PURCHASE'][Math.floor(Math.random() * 3)], 
            timestamp: new Date().toISOString()
        };
        
        await producer.send({
            topic: TOPIC_NAME,
            messages: [{ value: JSON.stringify(message) }], // JSON 문자열
        });

        console.log(`[Producer] Message sent: ${message.event_type} (ID: ${message.id})`);

    } catch (error) {
        console.error("Error sending message to Kafka:", error.message);
    }
};

const run = async () => {
    const MAX_ATTEMPTS = 10; // 최대 10번 시도 
    let attempt = 0;
    
    while (attempt < MAX_ATTEMPTS) {
        try {
            console.log(`[Producer] Attempting to connect to Kafka (Attempt ${attempt + 1}/${MAX_ATTEMPTS})...`);
            
            // 연결 시도 (kafkajs 내부 retry 포함)
            await producer.connect();
            
            console.log("[Producer] Successfully connected to Kafka!");
            
            // 연결 성공 시, 이벤트 실행
            setInterval(produceMessage, 1000); 
            return;
            
        } catch (error) {
            attempt++;
            console.error(`[Producer] Connection failed: ${error.message}. Retrying in 6 seconds...`);
            
            await new Promise(resolve => setTimeout(resolve, 6000));
        }
    }
    
    console.error("[Producer] FATAL ERROR: Could not connect to Kafka after multiple attempts. Exiting.");
    process.exit(1); 
};

run().catch(console.error);