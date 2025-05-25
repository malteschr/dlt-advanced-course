import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Define base URL
BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"

def jaffle_shop_pipeline():
    # Create REST client with pagination
    client = RESTClient(
        base_url=BASE_URL,
        paginator=PageNumberPaginator(
            base_page=1,
            page_param="page",
            total_path=None,
            stop_after_empty_page=True,
            maximum_page=5
        )
    )

    # Define resources that yield entire pages (chunking)
    @dlt.resource(name="customers", write_disposition="replace")
    def customers():
        for page in client.paginate("customers"):
            yield page

    @dlt.resource(name="orders", write_disposition="replace")
    def orders():
        for page in client.paginate("orders"):
            yield page

    @dlt.resource(name="products", write_disposition="replace")
    def products():
        for page in client.paginate("products"):
            yield page

    # Group resources into a source
    @dlt.source(name="jaffle_shop")
    def jaffle_shop_source():
        return [customers, orders, products]

    # Create pipeline with fixed name (no UUID)
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop",
        destination="duckdb",
        dataset_name="jaffle_shop_data",
        dev_mode=False
    )

    # Configure pipeline with optimized settings
    pipeline.run_settings = {
        'extract': {
            'buffer_max_items': 10000,
        },
        'normalize': {
            'file_rotation_size_mb': 100,
        }
    }

    return pipeline, jaffle_shop_source()

# This function will be the entry point for deployment
def run_pipeline():
    pipeline, source = jaffle_shop_pipeline()
    load_info = pipeline.run(source)
    return load_info

if __name__ == "__main__":
    run_pipeline()
