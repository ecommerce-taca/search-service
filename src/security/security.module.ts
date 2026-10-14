import { Module } from '@nestjs/common';
import { RolesGuard } from './roles.guard';
import { StepUpGuard } from './step-up.guard';

@Module({
  providers: [RolesGuard, StepUpGuard],
  exports: [RolesGuard, StepUpGuard],
})
export class SecurityModule {}
