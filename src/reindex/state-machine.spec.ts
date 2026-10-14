import { ReindexState } from '../common/enums';
import {
  assertTransition,
  canTransition,
  IllegalStateException,
  ReindexStateMachine,
} from './state-machine';

describe('ReindexStateMachine', () => {
  describe('valid transitions', () => {
    it('allows REQUESTED -> RUNNING, CANCELLED, FAILED', () => {
      expect(canTransition(ReindexState.REQUESTED, ReindexState.RUNNING)).toBe(
        true,
      );
      expect(
        canTransition(ReindexState.REQUESTED, ReindexState.CANCELLED),
      ).toBe(true);
      expect(canTransition(ReindexState.REQUESTED, ReindexState.FAILED)).toBe(
        true,
      );
      expect(() =>
        assertTransition(ReindexState.REQUESTED, ReindexState.RUNNING),
      ).not.toThrow();
    });

    it('allows RUNNING -> VALIDATING, CANCELLED, FAILED', () => {
      expect(canTransition(ReindexState.RUNNING, ReindexState.VALIDATING)).toBe(
        true,
      );
      expect(canTransition(ReindexState.RUNNING, ReindexState.CANCELLED)).toBe(
        true,
      );
      expect(canTransition(ReindexState.RUNNING, ReindexState.FAILED)).toBe(
        true,
      );
    });

    it('allows VALIDATING -> SWAPPED, CANCELLED, FAILED', () => {
      expect(canTransition(ReindexState.VALIDATING, ReindexState.SWAPPED)).toBe(
        true,
      );
      expect(
        canTransition(ReindexState.VALIDATING, ReindexState.CANCELLED),
      ).toBe(true);
      expect(canTransition(ReindexState.VALIDATING, ReindexState.FAILED)).toBe(
        true,
      );
    });
  });

  describe('illegal transitions', () => {
    it('rejects jumping from REQUESTED to SWAPPED', () => {
      expect(canTransition(ReindexState.REQUESTED, ReindexState.SWAPPED)).toBe(
        false,
      );
      expect(() =>
        assertTransition(ReindexState.REQUESTED, ReindexState.SWAPPED),
      ).toThrow(IllegalStateException);
    });

    it('rejects jumping from RUNNING to SWAPPED without VALIDATING', () => {
      expect(canTransition(ReindexState.RUNNING, ReindexState.SWAPPED)).toBe(
        false,
      );
      expect(() =>
        assertTransition(ReindexState.RUNNING, ReindexState.SWAPPED),
      ).toThrow(IllegalStateException);
    });

    it('rejects transitioning from terminal states', () => {
      expect(canTransition(ReindexState.SWAPPED, ReindexState.RUNNING)).toBe(
        false,
      );
      expect(canTransition(ReindexState.FAILED, ReindexState.SWAPPED)).toBe(
        false,
      );
      expect(canTransition(ReindexState.CANCELLED, ReindexState.RUNNING)).toBe(
        false,
      );

      expect(() =>
        assertTransition(ReindexState.SWAPPED, ReindexState.RUNNING),
      ).toThrow(IllegalStateException);
      expect(() =>
        assertTransition(ReindexState.FAILED, ReindexState.SWAPPED),
      ).toThrow(IllegalStateException);
      expect(() =>
        assertTransition(ReindexState.CANCELLED, ReindexState.RUNNING),
      ).toThrow(IllegalStateException);
    });

    it('rejects backwards transitions', () => {
      expect(
        canTransition(ReindexState.VALIDATING, ReindexState.REQUESTED),
      ).toBe(false);
      expect(canTransition(ReindexState.VALIDATING, ReindexState.RUNNING)).toBe(
        false,
      );
    });
  });

  describe('instance state machine', () => {
    it('manages lifecycle correctly', () => {
      const sm = new ReindexStateMachine();
      expect(sm.state).toBe(ReindexState.REQUESTED);

      sm.transitionTo(ReindexState.RUNNING);
      expect(sm.state).toBe(ReindexState.RUNNING);

      sm.transitionTo(ReindexState.VALIDATING);
      expect(sm.state).toBe(ReindexState.VALIDATING);

      sm.transitionTo(ReindexState.SWAPPED);
      expect(sm.state).toBe(ReindexState.SWAPPED);

      expect(() => sm.transitionTo(ReindexState.RUNNING)).toThrow(
        IllegalStateException,
      );
    });
  });
});
