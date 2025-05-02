import { APIGatewayProxyHandlerV2 } from "aws-lambda";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    console.log("Path Parameters:", event.pathParameters);
    
    // Extract path parameters for the crew/role/movies/movieId endpoint
    const role = event.pathParameters?.role;
    const movieId = event.pathParameters?.movieId;
    
    if (!role || !movieId) {
      return {
        statusCode: 400,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ 
          message: "Missing required path parameters: role and movieId" 
        }),
      };
    }
    
    const params = {
      TableName: process.env.TABLE_NAME,
      Key: {
        movieId: parseInt(movieId),
        role: role
      }
    };
    
    try {
      const response = await client.send(new GetCommand(params));
      console.log("DynamoDB Response:", JSON.stringify(response));
      
      if (!response.Item) {
        const scanParams = {
          TableName: process.env.TABLE_NAME,
          Limit: 5
        };
        
        console.log("Attempting scan to verify data exists");
        const scanResponse = await client.send(new QueryCommand({
          TableName: process.env.TABLE_NAME,
          KeyConditionExpression: "movieId = :movieId",
          ExpressionAttributeValues: {
            ":movieId": parseInt(movieId)
          },
          Limit: 5
        }));
        
        console.log("Scan response:", JSON.stringify(scanResponse));
        
        return {
          statusCode: 404,
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({ 
            message: `No crew member found with role '${role}' for movie '${movieId}'`,
            params: params,
            tableData: scanResponse.Items || []
          }),
        };
      }
      
      return {
        statusCode: 200,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify(response.Item),
      };
    } catch (dbError) {
      console.error("DynamoDB Error:", JSON.stringify(dbError));
      return {
        statusCode: 500,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ 
          message: "Error querying database",
          error: dbError,
          params: params
        }),
      };
    }
  } catch (error: any) {
    console.log("General Error:", JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}