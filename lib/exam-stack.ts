import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as custom from "aws-cdk-lib/custom-resources";
import { Construct } from "constructs";
import { generateBatch } from "../shared/util";
import { movieCrew } from "../seed/movies";
import * as apig from "aws-cdk-lib/aws-apigateway";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";

export class ExamStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Question 1 - Serverless REST API

    // A table that stores data about a movie's crew, i.e. director, camera operators, etc.
    const table = new dynamodb.Table(this, "MoviesTable", {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: "movieId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "role", type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      tableName: "ExamTable",
    });

    const question1Fn = new lambdanode.NodejsFunction(this, "Question1Fn", {
      architecture: lambda.Architecture.ARM_64,
      runtime: lambda.Runtime.NODEJS_22_X,
      entry: `${__dirname}/../lambdas/question1.ts`,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: {
        TABLE_NAME: table.tableName,
        REGION: "eu-west-1",
      },
    });

    // Grant the Lambda function read access to the DynamoDB table
    table.grantReadData(question1Fn);

    new custom.AwsCustomResource(this, "moviesddbInitData", {
      onCreate: {
        service: "DynamoDB",
        action: "batchWriteItem",
        parameters: {
          RequestItems: {
            [table.tableName]: generateBatch(movieCrew),
          },
        },
        physicalResourceId: custom.PhysicalResourceId.of("moviesddbInitData"), //.of(Date.now().toString()),
      },
      policy: custom.AwsCustomResourcePolicy.fromSdkCalls({
        resources: [table.tableArn],
      }),
    });

    const api = new apig.RestApi(this, "ExamAPI", {
      description: "Exam api",
      deployOptions: {
        stageName: "dev",
      },
      defaultCorsPreflightOptions: {
        allowHeaders: ["Content-Type", "X-Amz-Date"],
        allowMethods: ["OPTIONS", "GET", "POST", "PUT", "PATCH", "DELETE"],
        allowCredentials: true,
        allowOrigins: ["*"],
      },
    });

    const anEndpoint = api.root.addResource("patha");
    
    // New endpoints for getting crew member details by role and movie ID
    const crewResource = api.root.addResource("crew");
    const roleResource = crewResource.addResource("{role}");
    const moviesResource = roleResource.addResource("movies");
    const movieIdResource = moviesResource.addResource("{movieId}");
    
    console.log("API Endpoint Path:", `/crew/{role}/movies/{movieId}`);
    
    // Integrate the Lambda function with the API Gateway endpoint
    movieIdResource.addMethod(
      "GET",
      new apig.LambdaIntegration(question1Fn, {
        proxy: true,
      })
    );
    
  

    // ==================================
    // Question 2 - Event-Driven architecture

    const bucket = new s3.Bucket(this, "exam-bucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    const topic1 = new sns.Topic(this, "Topic1", {
      displayName: "Exam topic",
    });
    
    const queueB = new sqs.Queue(this, "QueueB", {
      receiveMessageWaitTime: cdk.Duration.seconds(5),
    });

    const queueA = new sqs.Queue(this, "queueA", {
      receiveMessageWaitTime: cdk.Duration.seconds(5),
    });
    
    const lambdaXFn = new lambdanode.NodejsFunction(this, "LambdaXFn", {
      architecture: lambda.Architecture.ARM_64,
      runtime: lambda.Runtime.NODEJS_22_X,
      entry: `${__dirname}/../lambdas/lambdaX.ts`,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: {
        REGION: "eu-west-1",
        QUEUE_B_URL: queueB.queueUrl,
      },
    });

    const lambdaYFn = new lambdanode.NodejsFunction(this, "LambdaYFn", {
      architecture: lambda.Architecture.ARM_64,
      runtime: lambda.Runtime.NODEJS_22_X,
      entry: `${__dirname}/../lambdas/lambdaY.ts`,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: {
        REGION: "eu-west-1",
      },
    });
    
    // Set up S3 bucket notification to SNS topic
    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(topic1)
    );
    
    // Connect SNS topic to SQS Queue A with a filter policy
    topic1.addSubscription(new subs.SqsSubscription(queueA, {
      rawMessageDelivery: true,
      filterPolicy: {
        "address.country": sns.SubscriptionFilter.stringFilter({
          allowlist: ["Ireland", "China"]
        }),
        
      },
    }));
    
    // Connect Lambda X to SQS Queue A as event source
    lambdaXFn.addEventSource(new events.SqsEventSource(queueA, {
      batchSize: 10
    }));
    
    // Grant Lambda X permission to send messages to Queue B
    queueB.grantSendMessages(lambdaXFn);
    
    // Connect Lambda Y to SQS Queue B as event source
    lambdaYFn.addEventSource(new events.SqsEventSource(queueB, {
      batchSize: 10
    }));
  }
}