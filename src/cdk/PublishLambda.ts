import { Duration, RemovalPolicy } from 'aws-cdk-lib';
import type { ITable } from 'aws-cdk-lib/aws-dynamodb';
import type { ITopic } from 'aws-cdk-lib/aws-sns';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { Runtime, StartingPosition, type IFunction } from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction, OutputFormat } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays, type ILogGroup } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { fileURLToPath } from 'url';
import path from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publishHandlerPath = path.join(__dirname, '..', 'lambdas', 'publish.lambda.ts');

type PublishLambdaProps = {
  readonly isProdEnv: boolean;
  readonly eventTable: ITable;
  readonly eventTopic: Pick<ITopic, 'topicArn' | 'grantPublish'>;
  readonly encryptionKey?: IKey;
};

export class PublishLambda extends Construct {
  public readonly function: IFunction;
  public readonly logGroup: ILogGroup;

  constructor(scope: Construct, props: PublishLambdaProps) {
    super(scope, 'PublishEvents');

    this.logGroup = new LogGroup(this, 'LogGroup', {
      retention: props.isProdEnv ? RetentionDays.SIX_MONTHS : RetentionDays.ONE_WEEK,
      removalPolicy: props.isProdEnv ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    });

    this.function = new NodejsFunction(this, 'PublishEvents', {
      entry: publishHandlerPath,
      description: 'Lambda to publish events to the event eventTopic',
      logGroup: this.logGroup,
      runtime: Runtime.NODEJS_22_X,
      memorySize: 512,
      timeout: Duration.seconds(5),
      environment: {
        LOG_LEVEL: 'INFO',
        NODE_OPTIONS: '--enable-source-maps',
        POWERTOOLS_DEV: props.isProdEnv ? 'false' : 'true',
        POWERTOOLS_LOGGER_LOG_EVENT: props.isProdEnv ? 'false' : 'true',
        ENV_TOPIC_ARN: props.eventTopic.topicArn,
      },
      bundling: {
        minify: true,
        sourceMap: true,
        metafile: true,
        format: OutputFormat.ESM,
        mainFields: ['module', 'main'],
        banner: 'import { createRequire } from "module"; const require = createRequire(import.meta.url);',
      },
    });

    this.function.addEventSource(
      new DynamoEventSource(props.eventTable, {
        startingPosition: StartingPosition.LATEST,
        reportBatchItemFailures: true,
      }),
    );

    props.eventTopic.grantPublish(this.function);

    if (props.encryptionKey) {
      props.encryptionKey.grantEncryptDecrypt(this.function);
    }
  }
}
