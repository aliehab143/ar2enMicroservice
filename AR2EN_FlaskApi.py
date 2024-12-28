from transformers import MarianMTModel, MarianTokenizer
from confluent_kafka import Consumer, Producer, KafkaError
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
REQUEST_TOPIC = 'translationArToEnRequest'
RESPONSE_TOPIC = 'translationArToEnResponse'

# Load Translation Model
MODEL_NAME = "Helsinki-NLP/opus-mt-ar-en"
print("[Model] Loading model...")
tokenizer = MarianTokenizer.from_pretrained(MODEL_NAME)
model = MarianMTModel.from_pretrained(MODEL_NAME)
print("[Model] Model loaded successfully.")

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'translation_worker_group',
    'auto.offset.reset': 'earliest',
}

# Kafka Producer Configuration
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

def translate_text(text):
    """Perform translation from Arabic to English."""
    print(f"[Translator] Translating text: {text}")
    translation = model.generate(**tokenizer(text, return_tensors="pt", padding=True))
    result = tokenizer.decode(translation[0], skip_special_tokens=True)
    print(f"[Translator] Translation result: {result}")
    return result

def kafka_consumer_worker():
    """Kafka consumer worker to process translation requests."""
    consumer = Consumer(consumer_conf)
    consumer.subscribe([REQUEST_TOPIC])
    print("[Consumer] Kafka Consumer Worker Started...")

    try:
        while True:
            # Poll for new messages
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[Consumer Error] {msg.error()}")
                    break

            # Process the received request
            try:
                request_data = json.loads(msg.value().decode("utf-8"))
                print(f"[Consumer] Received request: {request_data}")

                task_id = request_data.get("id")
                text = request_data.get("text")

                if not text:
                    print(f"[Consumer Error] Empty text in request: {request_data}")
                    continue

                # Perform translation
                translated_text = translate_text(text)

                # Send the result to the response topic
                response_data = {"id": task_id, "translated_text": translated_text, "status": "completed"}
                producer.produce(RESPONSE_TOPIC, json.dumps(response_data).encode("utf-8"))
                producer.flush()
                print(f"[Consumer] Sent response: {response_data}")
            except json.JSONDecodeError:
                print(f"[Consumer Error] Invalid message format: {msg.value()}")
            except Exception as e:
                print(f"[Consumer Error] Failed to process message: {e}")
    finally:
        consumer.close()
        print("[Consumer] Kafka Consumer Worker Stopped.")

if __name__ == "__main__":
    kafka_consumer_worker()