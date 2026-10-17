import { ConsumerState } from '../common/enums';

export interface DomainEvent<T = unknown> {
  event_id: string;
  schema_version: number;
  event_type: string;
  occurred_at: string | number;
  aggregate_type: string;
  aggregate_id: string;
  version: number;
  payload: T;
  replay_of_event_id?: string;
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

export interface KafkaMessageInput {
  headers?: Record<string, Buffer | string | undefined | (Buffer | string)[]>;
  value: Buffer | string | null;
  offset: string;
}

export interface DlqMetadata {
  partition?: number;
  offset?: string;
  attempts?: number;
  failed_at?: string;
}

export interface DlqEnvelope {
  event_id: string;
  event_type: string;
  aggregate_id: string;
  version: number;
  error_code: string;
  source_topic: string;
  source_partition: number;
  source_offset: string;
  attempts: number;
  failed_at: string;
}
