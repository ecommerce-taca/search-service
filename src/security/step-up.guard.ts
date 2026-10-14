import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { RequestContext } from '../common/context/request-context';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { STEP_UP_KEY } from './security.decorators';

@Injectable()
export class StepUpGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const requireStepUp = this.reflector.getAllAndOverride<boolean>(
      STEP_UP_KEY,
      [context.getHandler(), context.getClass()],
    );

    if (!requireStepUp) {
      return true;
    }

    const actor = RequestContext.getActor();
    if (!actor?.mfaStepUp) {
      throw new SearchException(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    }

    return true;
  }
}
