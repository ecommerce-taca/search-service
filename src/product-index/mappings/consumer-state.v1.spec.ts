import { CONSUMER_STATE_MAPPING_V1 } from './consumer-state.v1';

describe('CONSUMER_STATE_MAPPING_V1', () => {
  it('declares replay_of_event_id as keyword', () => {
    expect(CONSUMER_STATE_MAPPING_V1.properties).toBeDefined();
    expect(CONSUMER_STATE_MAPPING_V1.properties?.replay_of_event_id).toEqual({
      type: 'keyword',
    });
  });

  it('keeps dynamic strict and preserves existing field mappings', () => {
    expect(CONSUMER_STATE_MAPPING_V1.dynamic).toBe('strict');
    expect(CONSUMER_STATE_MAPPING_V1.properties?.event_id).toEqual({
      type: 'keyword',
    });
    expect(CONSUMER_STATE_MAPPING_V1.properties?.aggregate_id).toEqual({
      type: 'keyword',
    });
    expect(CONSUMER_STATE_MAPPING_V1.properties?.source_version).toEqual({
      type: 'long',
    });
    expect(CONSUMER_STATE_MAPPING_V1.properties?.status).toEqual({
      type: 'keyword',
    });
  });
});
