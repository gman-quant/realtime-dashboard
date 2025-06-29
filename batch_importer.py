import os
import orjson
import time
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point
from confluent_kafka import Consumer, KafkaError

# 設定批次大小，每 5000 筆資料寫入一次 InfluxDB
BATCH_SIZE = 5_000
# 連續多少次 poll() 沒有訊息才視為結束
MAX_NO_MESSAGE_STREAK = 5

def main():
    # --- 1. 從 .env 檔案載入設定 ---
    print("載入設定檔 .env ...")
    load_dotenv()
    
    INFLUXDB_URL = os.getenv("INFLUXDB_URL")
    INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
    INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    if not all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_DATABASE, KAFKA_BROKER, KAFKA_TOPIC]):
        print("錯誤：環境變數缺失，請檢查您的 .env 檔案。")
        return

    # --- 2. 初始化客戶端 ---
    try:
        print(f"正在連線至 InfluxDB: {INFLUXDB_URL}...")
        influx_client = InfluxDBClient3(host=INFLUXDB_URL, token=INFLUXDB_TOKEN, database=INFLUXDB_DATABASE)
        print("✅ 成功連線至 InfluxDB。")
    except Exception as e:
        print(f"❌ 連線至 InfluxDB 失敗: {e}")
        return

    kafka_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'importer-group-{time.time()}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(kafka_conf)
    print("✅ Kafka 消費者已建立。")

    # --- 3. 執行消費與寫入邏輯 ---
    point_batch = []
    message_count = 0
    start_time = time.time()
    no_message_streak = 0 # <--- 修正邏輯：連續無訊息計數器

    try:
        consumer.subscribe([KAFKA_TOPIC])
        print(f"已訂閱 Kafka 主題: {KAFKA_TOPIC}")
        print("開始從 Kafka 讀取歷史資料並匯入 InfluxDB...")

        while True:
            msg = consumer.poll(1.0) # 等待 1 秒

            if msg is None:
                no_message_streak += 1
                print(f"等待訊息中... (連續 {no_message_streak}/{MAX_NO_MESSAGE_STREAK} 次無訊息)")
                if no_message_streak >= MAX_NO_MESSAGE_STREAK:
                    print(f"已連續 {MAX_NO_MESSAGE_STREAK} 次未收到新訊息，視為處理完畢。")
                    break # 連續多次沒有訊息，才結束迴圈
                continue # 繼續下一次 poll，給消費者時間去協調

            no_message_streak = 0 # 只要收到任何訊息或事件，就重置計數器

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 這是一個正常的事件，代表某個分割區讀完了，繼續即可
                    print(f"資訊: 已到達分割區結尾 {msg.topic()} [{msg.partition()}]")
                    continue
                else:
                    print(f"Kafka 錯誤: {msg.error()}")
                    break # 遇到真正的錯誤才退出

            # 成功收到訊息，進行處理
            try:
                record = orjson.loads(msg.value())
                
                point = Point("txf_tick")
                for key, value in record.items():
                    if key == 'datetime':
                        point.time(value)
                    elif isinstance(value, (int, float)):
                        point.field(key, value)
                    # 您也可以根據需要將某些欄位設定為 tag 來加速查詢
                    # 例如，我們可以將 'tick_type' 和 'code' 設為 tag
                    elif key in ['tick_type', 'code', 'simtrade']:
                         point.tag(key, str(value))

                point_batch.append(point)
                message_count += 1
                
                # 每1000筆印一次日誌，讓你知道程式在動
                if message_count % 1000 == 0:
                    print(f"已處理 {message_count} 筆訊息...")

                if len(point_batch) >= BATCH_SIZE:
                    print(f"✅ 累積 {len(point_batch)} 筆資料，正在批次寫入 InfluxDB...")
                    influx_client.write(record=point_batch)
                    consumer.commit(asynchronous=False)
                    point_batch.clear()

            except orjson.JSONDecodeError:
                print(f"JSON 解碼錯誤，跳過此訊息: {msg.value()}")
            except Exception as e:
                print(f"處理訊息時發生錯誤: {e}")

    except KeyboardInterrupt:
        print("使用者中斷程序。")
    finally:
        if point_batch:
            print(f"✅ 程序結束，寫入最後 {len(point_batch)} 筆資料...")
            influx_client.write(record=point_batch)
            consumer.commit(asynchronous=False)
        
        end_time = time.time()
        print("\n--- 匯入完成 ---")
        print(f"總共處理了 {message_count} 筆訊息。")
        print(f"總耗時: {end_time - start_time:.2f} 秒。")

        consumer.close()
        influx_client.close()
        print("所有連線已關閉。")


if __name__ == "__main__":
    main()


