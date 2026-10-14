import { Module } from '@nestjs/common';
import { ElasticsearchModule } from './elasticsearch.module';
import { IndexBootstrapService } from './index-bootstrap.service';

@Module({
  imports: [ElasticsearchModule],
  providers: [IndexBootstrapService],
  exports: [ElasticsearchModule, IndexBootstrapService],
})
export class ProductIndexModule {}
