const { Kafka } = require('kafkajs');
const { Client } = require('@elastic/elasticsearch');

// 환경 변수 설정 (Docker Compose에서 주입됨)
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const ELASTICSEARCH_URL = process.env.ELASTICSEARCH_URL || 'http://localhost:9200';
const TOPIC_NAME = 'analytics_events';
const INDEX_NAME = 'analytics_data'; 
const MAX_ATTEMPTS = 10; // 연결 재시도 횟수

// Bulk 처리를 위한 상수 정의
const BULK_SIZE = 500; // 한 번에 보낼 최대 문서 개수
const BULK_INTERVAL_MS = 5000; // 최대 대기 시간 (5초)

// Bulk 버퍼 저장소
const bulkBuffer = []; 
let flushTimeout = null;

// Elasticsearch 클라이언트 설정
const esClient = new Client({ node: ELASTICSEARCH_URL });

// Kafka 클라이언트 설정
const kafka = new Kafka({ 
    clientId: 'analytics-consumer', 
    brokers: [KAFKA_BROKER] 
});
const consumer = kafka.consumer({ groupId: 'analytics-group' });

/**
 * 버퍼에 쌓인 모든 문서를 ElasticSearch로 한 번에 전송 (Flush)
 */
async function flushBuffer() {
    if (bulkBuffer.length === 0) return;

    // 타임아웃이 설정되어 있었다면 해제
    if (flushTimeout) {
        clearTimeout(flushTimeout);
        flushTimeout = null;
    }

    // 전송할 문서 복사 후 버퍼 비우기
    const documentsToSend = bulkBuffer.splice(0, bulkBuffer.length);
    
    // Bulk API 요청 본문 생성: 헤더(index)와 문서(document) 쌍으로 구성
    const operations = documentsToSend.flatMap(doc => [
        { index: { _index: doc.index } },
        doc.document 
    ]);

    try {
        const response = await esClient.bulk({ refresh: true, operations });

        if (response.errors) {
            // Bulk 요청 중 실패한 문서가 있다면 에러 처리 (실무에선 DLQ 등으로 분리)
            const failedItems = response.items.filter(item => item.index.error);
            console.error(`[Consumer] Bulk failed for ${failedItems.length} documents.`);
            console.error(failedItems[0].index.error); // 첫 번째 에러만 출력
        } else {
            console.log(`[Consumer] Successfully indexed ${documentsToSend.length} documents via Bulk API.`);
        }
    } catch (error) {
        console.error(`[Consumer] FATAL Bulk error: ${error.message}`);
        // 치명적 에러 발생 시 데이터 유실 방지를 위해 앱 종료 또는 경고 필요
    }
}

/**
 * Elasticsearch가 완전히 준비될 때까지 연결을 재시도하는 함수
 */
async function connectToElasticsearch() {
    let attempt = 0;
    while (attempt < MAX_ATTEMPTS) {
        try {
            console.log(`[Consumer] Attempting to connect to Elasticsearch (Attempt ${attempt + 1}/${MAX_ATTEMPTS})...`);
            
            await esClient.info(); // ES 연결 확인
            
            console.log("[Consumer] Successfully connected to Elasticsearch!");
            return; // 성공 시 함수 종료
            
        } catch (error) {
            attempt++;
            console.error(`[Consumer] ES Connection failed. Retrying in 6 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 6000));
        }
    }
    
    // 최종 실패
    console.error("[Consumer] FATAL ERROR: Could not connect to Elasticsearch. Exiting.");
    process.exit(1);
}

/**
 * 메인 실행 함수: ES 연결 후 Kafka 소비 시작
 */
const run = async () => {
    try {
        // 1. Elasticsearch 연결 (재시도 로직 포함)
        await connectToElasticsearch();
        
        // 2. Kafka 연결 및 소비 시작
        await consumer.connect();
        // fromBeginning: true는 Consumer Group이 처음 시작할 때 토픽의 가장 처음부터 데이터를 읽도록 설정
        await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

        await consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const event = JSON.parse(message.value.toString());
                
                // --- 데이터 전처리 (ETL) ---
                event.processing_time = new Date().toISOString(); 
                event.is_critical = event.event_type === 'PURCHASE'; 
                // -----------------------------
                
                // 1. 문서를 버퍼에 추가
                bulkBuffer.push({
                    index: INDEX_NAME,
                    document: event
                });

                // 2. 버퍼 크기가 임계값에 도달하면 즉시 전송
                if (bulkBuffer.length >= BULK_SIZE) {
                    await flushBuffer();
                } 
                
                // 3. (타이머가 없다면) 일정 시간 후 전송을 위한 타이머 설정
                if (!flushTimeout) {
                    flushTimeout = setTimeout(() => {
                        flushBuffer().catch(console.error); // 5초 후 남아있는 데이터 전송
                    }, BULK_INTERVAL_MS);
                }
                } catch (error) {
                     console.error(`[Consumer] Error processing message: ${error.message}`);
                }
            },
        });
    } catch (error) {
        // Kafka 연결 실패 등 다른 치명적 오류 처리
        console.error(`[Consumer] FATAL ERROR during run: ${error.message}`);
        process.exit(1);
    }
};

run().catch(console.error);