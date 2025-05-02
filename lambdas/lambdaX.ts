import { Handler } from "aws-lambda";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const sqsClient = new SQSClient({ region: process.env.REGION });
const queueBUrl = process.env.QUEUE_B_URL;

export const handler: Handler = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));

    // Process each message from Queue A
    for (const record of event.Records) {
      const messageBody = JSON.parse(record.body);
      
      let payload;
      if (messageBody.Message) {
        payload = JSON.parse(messageBody.Message);
      } else {
        payload = messageBody;
      }
      
      console.log("Processing payload:", JSON.stringify(payload, null, 2));
      
      if (!payload.email) {
        console.log("Payload is missing email property, forwarding to Queue B");
        
        await sqsClient.send(
          new SendMessageCommand({
            QueueUrl: queueBUrl,
            MessageBody: JSON.stringify(payload),
          })
        );
      } else {
        console.log("Payload has email property, skipping");
      }
    }

    return {
      statusCode: 200,
      body: "Processing complete"
    };
  } catch (error: any) {
    console.error("Error processing event:", error);
    throw new Error(JSON.stringify(error));
  }
};