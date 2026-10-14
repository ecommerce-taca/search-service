import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { createResponse } from '../common/response.helper';
import { AdminOnly } from '../security';
import { AdminSearchStatusQueryDto, ReindexRequestDto } from './dto';
import { ReindexService } from './reindex.service';

@Controller('admin/search')
export class AdminSearchController {
  constructor(private readonly reindexService: ReindexService) {}

  @Get('status')
  @AdminOnly()
  async getStatus(@Query() query: AdminSearchStatusQueryDto) {
    const data = await this.reindexService.getStatus(query);
    return createResponse(data);
  }

  @Post('reindex')
  @HttpCode(HttpStatus.ACCEPTED)
  @AdminOnly({ stepUp: true })
  async reindex(@Body() dto: ReindexRequestDto) {
    const data = await this.reindexService.start(dto);
    return createResponse(data);
  }

  @Get('reindex/:jobId')
  @AdminOnly()
  async getReindexJob(@Param('jobId') jobId: string) {
    const data = await this.reindexService.getJob(jobId);
    return createResponse(data);
  }

  @Post('reindex/:jobId/cancel')
  @HttpCode(HttpStatus.OK)
  @AdminOnly({ stepUp: true })
  async cancelReindexJob(@Param('jobId') jobId: string) {
    const data = await this.reindexService.cancel(jobId);
    return createResponse(data);
  }
}
