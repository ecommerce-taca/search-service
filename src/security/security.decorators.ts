import { applyDecorators, CustomDecorator, SetMetadata } from '@nestjs/common';

export const ROLES_KEY = 'roles';
export const RequireRoles = (...roles: string[]): CustomDecorator<string> =>
  SetMetadata(ROLES_KEY, roles);

export const STEP_UP_KEY = 'step_up';
export const RequireStepUp = (): CustomDecorator<string> =>
  SetMetadata(STEP_UP_KEY, true);

export interface AdminOnlyOptions {
  stepUp?: boolean;
}

export function AdminOnly(options?: AdminOnlyOptions) {
  if (options?.stepUp) {
    return applyDecorators(RequireRoles('SEARCH_ADMIN'), RequireStepUp());
  }
  return applyDecorators(RequireRoles('SEARCH_ADMIN'));
}
