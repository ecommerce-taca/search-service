import { Global, Module } from '@nestjs/common';
import { EsCategoryProjection } from './category-projection.service';
import { ElasticsearchModule } from './elasticsearch.module';
import { IndexBootstrapService } from './index-bootstrap.service';
import { CATEGORY_PROJECTION } from './interfaces';

@Global()
@Module({
  imports: [ElasticsearchModule],
  providers: [
    IndexBootstrapService,
    {
      provide: CATEGORY_PROJECTION,
      useClass: EsCategoryProjection,
    },
    EsCategoryProjection,
  ],
  exports: [
    ElasticsearchModule,
    IndexBootstrapService,
    CATEGORY_PROJECTION,
    EsCategoryProjection,
  ],
})
export class ProductIndexModule {}
