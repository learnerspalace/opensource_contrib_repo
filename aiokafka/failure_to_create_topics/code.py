import asyncio
import uuid

from aiokafka.admin import AIOKafkaAdminClient, NewPartitions, NewTopic


async def main():
    random_topic_name = "topic-" + str(uuid.uuid4())
    # create a new topic
    admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
    await admin.start()
    iteration = 0
    while True:
        response = await admin.create_topics(
            [NewTopic(name=random_topic_name, num_partitions=1, replication_factor=1)]
        )
        print(f"----------{iteration=}-----------")
        # if error_code is not 0, then topic creation failed
        if any(
            error_code for (topic, error_code, error_message) in response.topic_errors
        ):
            print(
                f"Error creating topic as request sent to non-controller Node :: error_code='{response.topic_errors[0][1]}' error_message='{response.topic_errors[0][2]}'"
            )
            print("Retrying topic creation until successful...")

        # if error_code is 0, then topic creation was successful
        if not any(
            error_code for (topic, error_code, error_message) in response.topic_errors
        ):
            print(
                f"Topic '{random_topic_name}' created successfully. Request sent to controller Node."
            )
            break
        iteration += 1


async def check_version():
    admin: AIOKafkaAdminClient = AIOKafkaAdminClient(
        bootstrap_servers="localhost:9092",
    )
    await admin.start()
    version = await admin._get_cluster_metadata()
    print(version)


# asyncio.run(check_version())
asyncio.run(main())
