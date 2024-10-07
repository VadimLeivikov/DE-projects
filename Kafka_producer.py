from time import sleep
from json import dumps
from kafka import KafkaProducer
from random import randrange, choices
import string

# Callback function for successful message delivery:
def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} with offset {record_metadata.offset}")

# Callback function for error:
def on_send_error(excp):
    print(f"Message delivery failed: {excp}")

producer = KafkaProducer(bootstrap_servers=['your_ip_address_here:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         retries=5)

def push():
    for e in range(50):
        text = ''.join(choices(string.ascii_uppercase + string.digits, k=20))
        user = {'id': randrange(5), 'action': text}
        # Sending a message with handlers:
        producer.send('netology-spark', value=user).add_callback(on_send_success).add_errback(on_send_error)
        sleep(randrange(3))
        print("Produced message ", e)

try:
    push()
except KeyboardInterrupt:
    producer.close()
    print("Exit")


