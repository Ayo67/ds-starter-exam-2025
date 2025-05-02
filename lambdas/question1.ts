import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    
    // Extract path parameters for the crew/role/movies/movieId endpoint
    const role = event.pathParameters?.role;
    const movieId = event.pathParameters?.movieId;
    const verbose = event.queryStringParameters?.verbose === 'true';
    
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
    const movieIdNumber = parseInt(movieId);
    
    if (verbose) {
      const queryParams = {
        TableName: process.env.TABLE_NAME,
        KeyConditionExpression: "movieId = :movieId",
        ExpressionAttributeValues: {
          ":movieId": movieIdNumber
        }
      };
      
      const response = await client.send(new QueryCommand(queryParams));
      
      if (!response.Items || response.Items.length === 0) {
        return {
          statusCode: 404,
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({ 
            message: `No crew members found for movie '${movieId}'`
          }),
        };
      }
      
      return {
        statusCode: 200,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          movieId: movieIdNumber,
          crew: response.Items
        }),
      };
    } else {
      // Non-verbose mode - Get specific crew member by role
      const params = {
        TableName: process.env.TABLE_NAME,
        Key: {
          movieId: movieIdNumber,
          role: role
        }
      };
      
      const response = await client.send(new GetCommand(params));
      
      if (!response.Item) {
        return {
          statusCode: 404,
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({ 
            message: `No crew member found with role '${role}' for movie '${movieId}'`
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
    }
  } catch (error: any) {
    console.log("Error:", JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ 
        message: "An error occurred",
        error: error.message 
      }),
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