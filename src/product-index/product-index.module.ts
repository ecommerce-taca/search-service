import { Global, Module } from '@nestjs/common';
import { ElasticsearchModule } from './elasticsearch.module';
import { IndexBootstrapService } from './index-bootstrap.service';

@Global()
@Module({
  imports: [ElasticsearchModule],
  providers: [IndexBootstrapService],
  exports: [ElasticsearchModule, IndexBootstrapService],
})
export class ProductIndexModule {}
