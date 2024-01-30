try:
    from kafka import KafkaProducer
    from faker import Faker
    import json
    from time import sleep
except Exception as e:
    pass

producer = KafkaProducer(bootstrap_servers='3.82.243.106:9093')
_instance = Faker()


for _ in range(200):
    _data = {
        "first_name": _instance.first_name(),
        "city":_instance.city(),
        "phone_number":_instance.phone_number(),
        "state":_instance.state(),
        "id":str(_)
    }
    _payload = json.dumps(_data).encode("utf-8")
    response = producer.send('FirstTopic', _payload)
    print(response)

    sleep(2)

