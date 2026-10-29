
from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="anna_malkova_hw05"
)

print("\nKafka topics:")

for topic in sorted(admin_client.list_topics()):
    if "anna_malkova" in topic:
        print(topic)

admin_client.close()