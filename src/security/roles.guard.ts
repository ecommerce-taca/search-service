import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { RequestContext } from '../common/context/request-context';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { ROLES_KEY } from './security.decorators';

const SUPER_ADMIN_ROLE = 'SUPER_ADMIN';

@Injectable()
export class RolesGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const requiredRoles = this.reflector.getAllAndOverride<string[]>(
      ROLES_KEY,
      [context.getHandler(), context.getClass()],
    );

    if (!requiredRoles || requiredRoles.length === 0) {
      return true;
    }

    const actor = RequestContext.getActor();
    const userRoles = actor?.roles || [];
    const userPermissions = actor?.permissions || [];

    // auth-user cấp SEARCH_ADMIN dưới dạng permission (X-User-Permissions); vẫn chấp nhận
    // dạng role cho tương thích. SUPER_ADMIN (role admin của auth-user) qua mọi yêu cầu.
    if (userRoles.includes(SUPER_ADMIN_ROLE)) {
      return true;
    }
    const hasAllRoles = requiredRoles.every(
      (role) => userRoles.includes(role) || userPermissions.includes(role),
    );
    if (!hasAllRoles) {
      throw new SearchException(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
    }

    return true;
  }
}
