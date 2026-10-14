import { ReindexState } from '../common/enums';
import {
  ALLOWED_REINDEX_TRANSITIONS,
  IllegalStateTransitionException,
  ReindexStateMachine,
} from './state-machine';

describe('ReindexStateMachine (Unit)', () => {
  let stateMachine: ReindexStateMachine;

  beforeEach(() => {
    stateMachine = new ReindexStateMachine();
  });

  describe('Allowed Transitions', () => {
    it('[RX-U-22] State machine: transition REQUESTED -> RUNNING updates state and timestamp', () => {
      const result = stateMachine.transition(
        ReindexState.REQUESTED,
        ReindexState.RUNNING,
      );
      expect(result).toBe(ReindexState.RUNNING);
    });

    it('[RX-U-23] State machine: transition RUNNING -> VALIDATING updates state upon replay/copy completion', () => {
      const result = stateMachine.transition(
        ReindexState.RUNNING,
        ReindexState.VALIDATING,
      );
      expect(result).toBe(ReindexState.VALIDATING);
    });

    it('[RX-U-24] State machine: transition VALIDATING -> SWAPPED updates state upon successful alias swap', () => {
      const result = stateMachine.transition(
        ReindexState.VALIDATING,
        ReindexState.SWAPPED,
      );
      expect(result).toBe(ReindexState.SWAPPED);
    });

    it('[RX-U-25] State machine: transition REQUESTED -> FAILED when initialization fails', () => {
      const result = stateMachine.transition(
        ReindexState.REQUESTED,
        ReindexState.FAILED,
      );
      expect(result).toBe(ReindexState.FAILED);
    });

    it('[RX-U-26] State machine: transition RUNNING -> FAILED on unrecoverable copy or kafka replay error', () => {
      const result = stateMachine.transition(
        ReindexState.RUNNING,
        ReindexState.FAILED,
      );
      expect(result).toBe(ReindexState.FAILED);
    });

    it('[RX-U-27] State machine: transition VALIDATING -> FAILED on pre-swap gate failure', () => {
      const result = stateMachine.transition(
        ReindexState.VALIDATING,
        ReindexState.FAILED,
      );
      expect(result).toBe(ReindexState.FAILED);
    });

    it('[RX-U-28] State machine: transition REQUESTED / RUNNING / VALIDATING -> CANCELLED upon operator cancel signal', () => {
      expect(
        stateMachine.transition(ReindexState.REQUESTED, ReindexState.CANCELLED),
      ).toBe(ReindexState.CANCELLED);
      expect(
        stateMachine.transition(ReindexState.RUNNING, ReindexState.CANCELLED),
      ).toBe(ReindexState.CANCELLED);
      expect(
        stateMachine.transition(
          ReindexState.VALIDATING,
          ReindexState.CANCELLED,
        ),
      ).toBe(ReindexState.CANCELLED);
    });
  });

  describe('Illegal Transitions', () => {
    it('[RX-U-29] State machine: illegal transition REQUESTED -> SWAPPED throws exception', () => {
      expect(() =>
        stateMachine.transition(ReindexState.REQUESTED, ReindexState.SWAPPED),
      ).toThrow(IllegalStateTransitionException);
    });

    it('[RX-U-30] State machine: illegal transition RUNNING -> SWAPPED throws exception', () => {
      expect(() =>
        stateMachine.transition(ReindexState.RUNNING, ReindexState.SWAPPED),
      ).toThrow(IllegalStateTransitionException);
    });

    it('[RX-U-31] State machine: illegal transition SWAPPED -> RUNNING throws exception (terminal state immutable)', () => {
      expect(() =>
        stateMachine.transition(ReindexState.SWAPPED, ReindexState.RUNNING),
      ).toThrow(IllegalStateTransitionException);
    });

    it('[RX-U-32] State machine: illegal transition FAILED -> SWAPPED throws exception (terminal state immutable)', () => {
      expect(() =>
        stateMachine.transition(ReindexState.FAILED, ReindexState.SWAPPED),
      ).toThrow(IllegalStateTransitionException);
    });

    it('[RX-U-33] State machine: illegal transition CANCELLED -> RUNNING throws exception (terminal state immutable)', () => {
      expect(() =>
        stateMachine.transition(ReindexState.CANCELLED, ReindexState.RUNNING),
      ).toThrow(IllegalStateTransitionException);
    });

    it('[RX-U-34] State machine: illegal transition VALIDATING -> REQUESTED throws exception (forward-only)', () => {
      expect(() =>
        stateMachine.transition(
          ReindexState.VALIDATING,
          ReindexState.REQUESTED,
        ),
      ).toThrow(IllegalStateTransitionException);
    });

    it('[RX-U-35] State machine: illegal transition VALIDATING -> RUNNING throws exception (backward transition rejected)', () => {
      expect(() =>
        stateMachine.transition(ReindexState.VALIDATING, ReindexState.RUNNING),
      ).toThrow(IllegalStateTransitionException);
    });
  });

  describe('Comprehensive State Transition Matrix & Terminal States', () => {
    it('verifies allowed transition map matches specification exactly', () => {
      expect(ALLOWED_REINDEX_TRANSITIONS[ReindexState.REQUESTED]).toEqual(
        expect.arrayContaining([
          ReindexState.RUNNING,
          ReindexState.CANCELLED,
          ReindexState.FAILED,
        ]),
      );
      expect(ALLOWED_REINDEX_TRANSITIONS[ReindexState.RUNNING]).toEqual(
        expect.arrayContaining([
          ReindexState.VALIDATING,
          ReindexState.CANCELLED,
          ReindexState.FAILED,
        ]),
      );
      expect(ALLOWED_REINDEX_TRANSITIONS[ReindexState.VALIDATING]).toEqual(
        expect.arrayContaining([
          ReindexState.SWAPPED,
          ReindexState.CANCELLED,
          ReindexState.FAILED,
        ]),
      );
      expect(ALLOWED_REINDEX_TRANSITIONS[ReindexState.SWAPPED]).toEqual([]);
      expect(ALLOWED_REINDEX_TRANSITIONS[ReindexState.FAILED]).toEqual([]);
      expect(ALLOWED_REINDEX_TRANSITIONS[ReindexState.CANCELLED]).toEqual([]);
    });

    it('rejects any transition from terminal states SWAPPED, FAILED, CANCELLED to any state', () => {
      const allStates = Object.values(ReindexState);
      const terminalStates = [
        ReindexState.SWAPPED,
        ReindexState.FAILED,
        ReindexState.CANCELLED,
      ];

      for (const terminal of terminalStates) {
        expect(stateMachine.isTerminal(terminal)).toBe(true);
        for (const target of allStates) {
          expect(() => stateMachine.transition(terminal, target)).toThrow();
        }
      }
    });

    it('canTransition returns true for legal transitions and false for illegal ones', () => {
      expect(
        stateMachine.canTransition(
          ReindexState.REQUESTED,
          ReindexState.RUNNING,
        ),
      ).toBe(true);
      expect(
        stateMachine.canTransition(
          ReindexState.REQUESTED,
          ReindexState.SWAPPED,
        ),
      ).toBe(false);
      expect(
        stateMachine.canTransition(
          ReindexState.RUNNING,
          ReindexState.VALIDATING,
        ),
      ).toBe(true);
      expect(
        stateMachine.canTransition(
          ReindexState.VALIDATING,
          ReindexState.SWAPPED,
        ),
      ).toBe(true);
      expect(
        stateMachine.canTransition(
          ReindexState.SWAPPED,
          ReindexState.REQUESTED,
        ),
      ).toBe(false);
    });
  });
});
