import boto3

import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    logger.info(f"{event=}")
    try:
        instance_details = event["detail"]
        instance_id = instance_details["instance-id"]
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table("Assignment-1")
        sns_topic_arn = os.environ["SNS_TOPIC_ARN"]

        #Check if record exists
        resp = table.get_item(Key={"instance_id": instance_id})
        if "Item" in resp:
            # Record exists
            table.update_item(
                Key={"instance_id": instance_id},
                UpdateExpression="SET instance_current_state = :val1",
                ExpressionAttributeValues={":val1": instance_details["state"]}
            )
            logger.info(f"Record with instance_id = {instance_id} updated with state {instance_details["state"]}")
        else:
            if instance_details["state"] == "pending":
                # Newly created EC2
                record = {
                    "instance_id": instance_id,
                    "instance_launch_time": event["time"],
                    "instance_current_state": instance_details["state"]
                }
                table.put_item(Item=record)
                logger.info(f"New record added: {record=}")
                sns = boto3.client('sns')
                sns.publish(TopicArn=sns_topic_arn, Message=f"New Instance launched with instance ID: {instance_id}")
                logger.info(f"SNS notification send to topic ARN {sns_topic_arn}")
    except Exception as e:
        raise e
