import { Global, Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { RolesGuard } from './roles.guard';
import { StepUpGuard } from './step-up.guard';

@Global()
@Module({
  providers: [
    RolesGuard,
    StepUpGuard,
    {
      provide: APP_GUARD,
      useClass: RolesGuard,
    },
    {
      provide: APP_GUARD,
      useClass: StepUpGuard,
    },
  ],
  exports: [RolesGuard, StepUpGuard],
})
export class SecurityModule {}
