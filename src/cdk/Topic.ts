import { AccountPrincipal, type IGrantable } from 'aws-cdk-lib/aws-iam';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { type ITopic, Topic as CdkTopic, TracingConfig } from 'aws-cdk-lib/aws-sns';
import { Construct } from 'constructs';

export type TopicProps = {
  readonly description: string;
  /** AWS Account IDs */
  readonly subscriptionAccounts: string[];
  readonly encryptionKey?: IKey;
};

export class Topic extends Construct {
  public readonly topic: ITopic;

  constructor(scope: Construct, id: string, props: TopicProps) {
    super(scope, id);

    this.topic = new CdkTopic(this, id, {
      topicName: id,
      displayName: props.description,
      masterKey: props.encryptionKey,
      enforceSSL: true,
      tracingConfig: TracingConfig.ACTIVE,
    });

    for (const id of props.subscriptionAccounts) {
      this.topic.grantSubscribe(new AccountPrincipal(id));
    }
  }

  public get topicArn(): string {
    return this.topic.topicArn;
  }

  public grantPublish(grantee: IGrantable) {
    this.topic.grantPublish(grantee);
  }
}
