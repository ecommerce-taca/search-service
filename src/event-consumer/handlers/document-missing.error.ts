/**
 * Raised when an event needs an indexed product document that does not exist
 * yet. Catalog publishes product, catalog and sku events on different topics
 * that are consumed independently, so an update can legitimately arrive before
 * `product.created`. The event processor parks such events and replays them
 * once the document exists.
 */
export class DocumentMissingError extends Error {
  constructor(readonly productId: string) {
    super(`Product ${productId} is not indexed yet`);
    this.name = 'DocumentMissingError';
  }
}
