import { ConsumerState } from '../common/enums';

export interface DomainEvent<T = unknown> {
  event_id: string;
  schema_version: number;
  event_type: string;
  occurred_at: string;
  aggregate_type: string;
  aggregate_id: string;
  version: number;
  payload: T;
}

export interface ConsumerStatus {
  state: ConsumerState;
  lagByTopic: Record<string, number>;
  maxLagSeconds: number;
  lastEventAt: string | null;
  dlqCount: number;
}

export interface IConsumerStatusProvider {
  getStatus(): Promise<ConsumerStatus> | ConsumerStatus;
}

export const CONSUMER_STATUS_PROVIDER = 'IConsumerStatusProvider';
