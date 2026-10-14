import { CustomDecorator, SetMetadata } from '@nestjs/common';

export const ROLES_KEY = 'roles';
export const RequireRoles = (...roles: string[]): CustomDecorator<string> =>
  SetMetadata(ROLES_KEY, roles);

export const STEP_UP_KEY = 'step_up';
export const RequireStepUp = (): CustomDecorator<string> =>
  SetMetadata(STEP_UP_KEY, true);
