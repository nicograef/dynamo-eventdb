import { Duration, RemovalPolicy } from 'aws-cdk-lib';
import type { ITable } from 'aws-cdk-lib/aws-dynamodb';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { Runtime, StartingPosition, Tracing, type IFunction } from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction, OutputFormat } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays, type ILogGroup } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

export type LambdaProps = {
  readonly isProdEnv: boolean;
  readonly handlerPath: string;
  readonly description: string;

  readonly encryptionKey?: IKey;
  readonly timeoutSeconds?: number;
  readonly memorySizeMB?: number;
  readonly reservedConcurrentExecutions?: number;
  readonly environment?: Record<string, string>;
};

export class Lambda extends Construct {
  public readonly function: IFunction;
  public readonly logGroup: ILogGroup;

  constructor(scope: Construct, id: string, props: LambdaProps) {
    super(scope, id);

    this.logGroup = new LogGroup(this, 'LogGroup', {
      retention: props.isProdEnv ? RetentionDays.SIX_MONTHS : RetentionDays.ONE_WEEK,
      removalPolicy: props.isProdEnv ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    });

    this.function = new NodejsFunction(this, id, {
      entry: props.handlerPath,
      description: props.description,
      logGroup: this.logGroup,
      runtime: Runtime.NODEJS_22_X,
      memorySize: props.memorySizeMB ?? 1024,
      timeout: props.timeoutSeconds ? Duration.seconds(props.timeoutSeconds) : undefined,
      reservedConcurrentExecutions: props.reservedConcurrentExecutions,
      tracing: Tracing.ACTIVE,
      environment: {
        // This is necessary to avoid log errors when using clients in the setup code outside of a handler.
        AWS_XRAY_CONTEXT_MISSING: 'IGNORE_ERROR',
        LOG_LEVEL: 'INFO',
        NODE_OPTIONS: '--enable-source-maps',
        POWERTOOLS_DEV: props.isProdEnv ? 'false' : 'true',
        POWERTOOLS_LOGGER_LOG_EVENT: props.isProdEnv ? 'false' : 'true',
        ...props.environment,
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

    if (props.encryptionKey) {
      props.encryptionKey.grantEncryptDecrypt(this.function);
    }
  }

  public addDynamoEventSource(table: ITable) {
    this.function.addEventSource(
      new DynamoEventSource(table, {
        startingPosition: StartingPosition.LATEST,
        reportBatchItemFailures: true,
      }),
    );
  }
}
