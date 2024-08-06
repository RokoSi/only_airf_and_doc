from confluent_kafka import Consumer, Producer, KafkaException


def main():
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',  # Адрес Kafka брокера
        'group.id': 'saver',  # Идентификатор группы консумеров
        'auto.offset.reset': 'earliest'  # Начинаем читать сообщения с самого начала, если смещение не найдено
    })
    consumer.subscribe(['parsed_users'])


if __name__ == "__main__":
    main()
