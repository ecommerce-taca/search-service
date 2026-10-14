import { ReindexState } from '../common/enums';

export class IllegalStateException extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'IllegalStateException';
  }
}

export const IllegalStateTransitionException = IllegalStateException;
export type IllegalStateTransitionException = IllegalStateException;

export const ALLOWED_TRANSITIONS: Readonly<
  Record<ReindexState, readonly ReindexState[]>
> = {
  [ReindexState.REQUESTED]: [
    ReindexState.RUNNING,
    ReindexState.CANCELLED,
    ReindexState.FAILED,
  ],
  [ReindexState.RUNNING]: [
    ReindexState.VALIDATING,
    ReindexState.CANCELLED,
    ReindexState.FAILED,
  ],
  [ReindexState.VALIDATING]: [
    ReindexState.SWAPPED,
    ReindexState.CANCELLED,
    ReindexState.FAILED,
  ],
  [ReindexState.SWAPPED]: [],
  [ReindexState.FAILED]: [],
  [ReindexState.CANCELLED]: [],
};

export const ALLOWED_REINDEX_TRANSITIONS = ALLOWED_TRANSITIONS;

export function canTransition(from: ReindexState, to: ReindexState): boolean {
  return ALLOWED_TRANSITIONS[from]?.includes(to) ?? false;
}

export function assertTransition(from: ReindexState, to: ReindexState): void {
  if (!canTransition(from, to)) {
    throw new IllegalStateException(
      `Cannot transition reindex job from ${from} to ${to}`,
    );
  }
}

export class ReindexStateMachine {
  private currentState: ReindexState;

  constructor(initialState: ReindexState = ReindexState.REQUESTED) {
    this.currentState = initialState;
  }

  get state(): ReindexState {
    return this.currentState;
  }

  canTransitionTo(to: ReindexState): boolean {
    return canTransition(this.currentState, to);
  }

  transitionTo(to: ReindexState): ReindexState {
    assertTransition(this.currentState, to);
    this.currentState = to;
    return this.currentState;
  }

  transition(from: ReindexState, to: ReindexState): ReindexState {
    assertTransition(from, to);
    this.currentState = to;
    return to;
  }

  isTerminal(state: ReindexState): boolean {
    return ALLOWED_TRANSITIONS[state]?.length === 0;
  }

  canTransition(from: ReindexState, to: ReindexState): boolean {
    return canTransition(from, to);
  }

  static assertTransition(from: ReindexState, to: ReindexState): void {
    assertTransition(from, to);
  }

  static canTransition(from: ReindexState, to: ReindexState): boolean {
    return canTransition(from, to);
  }
}
