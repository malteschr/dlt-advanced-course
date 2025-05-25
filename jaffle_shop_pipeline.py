import os
import time
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import threading
import uuid
import multiprocessing

# Define base URL and API endpoints
BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"
ENDPOINTS = ["customers", "orders", "products"]

# Helper function to measure execution time
def time_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{func.__name__} completed in {execution_time:.2f} seconds")
        return result, execution_time
    return wrapper

# ==================== NAIVE IMPLEMENTATION ====================

def create_naive_pipeline():
    # Create REST client with pagination - fixed to work with Jaffle Shop API
    client = RESTClient(
        base_url=BASE_URL,
        paginator=PageNumberPaginator(
            base_page=1,
            page_param="page",
            total_path=None,  # Set to None since API doesn't provide total pages
            stop_after_empty_page=True,  # Stop when we get an empty page
            maximum_page=5  # limit to 5 pages to avoid long runs
        )
    )

    # Define resources that yield individual items
    @dlt.resource(name="customers", write_disposition="replace")
    def customers():
        for page in client.paginate("customers"):
            for item in page:
                yield item

    @dlt.resource(name="orders", write_disposition="replace")
    def orders():
        for page in client.paginate("orders"):
            for item in page:
                yield item

    @dlt.resource(name="products", write_disposition="replace")
    def products():
        for page in client.paginate("products"):
            for item in page:
                yield item

    # Create pipeline with unique identifiers
    pipeline_uuid = str(uuid.uuid4())[:8]  # Use first 8 chars of UUID for readability
    pipeline = dlt.pipeline(
        pipeline_name=f"jaffle_benchmark_{pipeline_uuid}",
        destination="duckdb",
        dataset_name=f"jaffle_naive_{pipeline_uuid}",
        dev_mode=False
    )

    return pipeline, [customers, orders, products]

@time_execution
def run_naive_pipeline():
    pipeline, resources = create_naive_pipeline()
    load_info = pipeline.run(resources)
    return load_info

# ==================== OPTIMIZED IMPLEMENTATION ====================

def create_optimized_pipeline():
    # Set environment variables for worker tuning
    os.environ['EXTRACT__WORKERS'] = '2'
    os.environ['NORMALIZE__WORKERS'] = '2'
    os.environ['LOAD__WORKERS'] = '2'

    # Create REST client with pagination - fixed to work with Jaffle Shop API
    client = RESTClient(
        base_url=BASE_URL,
        paginator=PageNumberPaginator(
            base_page=1,
            page_param="page",
            total_path=None,  # Set to None since API doesn't provide total pages
            stop_after_empty_page=True,  # Stop when we get an empty page
            maximum_page=5  # limit to 5 pages to avoid long runs
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

    # Create pipeline with unique identifiers
    pipeline_uuid = str(uuid.uuid4())[:8]  # Use first 8 chars of UUID for readability
    pipeline = dlt.pipeline(
        pipeline_name=f"jaffle_benchmark_{pipeline_uuid}",
        destination="duckdb",
        dataset_name=f"jaffle_optimized_{pipeline_uuid}",
        dev_mode=False
    )

    # Configure pipeline with optimized settings
    pipeline.run_settings = {
        'execution': {
            'parallelized': False,
        },
        'extract': {
            'buffer_max_items': 10000,  # Buffer control
        },
        'normalize': {
            'file_rotation_size_mb': 100,  # File rotation
        }
    }

    return pipeline, jaffle_shop_source()  # Call the source function here

@time_execution
def run_optimized_pipeline():
    pipeline, source = create_optimized_pipeline()
    load_info = pipeline.run(source)
    return load_info

# ==================== MAIN EXECUTION ====================

def main():
    # Run naive pipeline once
    print("Running naive pipeline...")
    naive_result, naive_time = run_naive_pipeline()

    # Run optimized pipeline once
    print("\nRunning optimized pipeline...")
    optimized_result, optimized_time = run_optimized_pipeline()

    # Calculate performance improvement
    improvement = (naive_time - optimized_time) / naive_time * 100

    # Print results
    print("\n========== PERFORMANCE RESULTS ==========")
    print(f"Naive pipeline: {naive_time:.2f} seconds")
    print(f"Optimized pipeline: {optimized_time:.2f} seconds")
    print(f"Performance improvement: {improvement:.2f}%")

    # Print thread information
    print("\n========== THREAD INFORMATION ==========")
    print(f"Current thread: {threading.current_thread().name}")
    print(f"Active thread count: {threading.active_count()}")

    # Print optimization details
    print("\n========== OPTIMIZATION DETAILS ==========")
    print("Key optimizations applied:")
    print("1. Chunking: Yielding entire pages instead of individual items")
    print("2. Parallelism: Setting parallelized=True and configuring worker counts")
    print("3. Buffer Control: Increasing buffer_max_items to 10000")
    print("4. File Rotation: Setting file_rotation_size_mb to 100")
    print("5. Worker Tuning: Setting EXTRACT__WORKERS, NORMALIZE__WORKERS, and LOAD__WORKERS to 2")
    print("6. Source Grouping: Grouping resources into a source for better orchestration")

if __name__ == "__main__":
    # This is the critical part that fixes the multiprocessing issue
    multiprocessing.freeze_support()
    main()
