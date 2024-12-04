import time
from flow import flow
import pandas as pd
import os
from utils_local import list_folders, list_all_files, filter_input_by_timeframe, json_to_dataframe

def compare_flow_performance(from_date, to_date, model, model_code, iterations=5):
    """
    Compare performance between JSON and Parquet processing for flow function
    """
    json_times = []
    parquet_times = []
    
    print(f"Running performance test with {iterations} iterations...")
    print(f"Time period: {from_date} to {to_date}")
    print(f"Model: {model}, Code: {model_code}")
    print("-" * 50)
    
    # Check for files in both directory structure and single file
    main_folder = 'analytics/snapshots'
    single_parquet = main_folder + '/all_data.parquet'
    
    # Check directory structure
    if os.path.exists(main_folder):
        dates = list_folders(main_folder)
        all_files = list_all_files(main_folder, dates)
        json_files = [f for f in all_files if f.endswith('.json')]
        json_files = filter_input_by_timeframe(json_files, from_date, to_date)
    else:
        json_files = []
        
    # Check for single Parquet file
    if os.path.exists(single_parquet):
        print(f"Found Parquet file at: {os.path.abspath(single_parquet)}")
        parquet_files = [single_parquet]
    else:
        print(f"Parquet file not found at: {os.path.abspath(single_parquet)}")
        parquet_files = []
        
    print(f"Found {len(json_files)} JSON files and {len(parquet_files)} Parquet files")
    
    # Test JSON performance if files exist
    if json_files:
        print("\nTesting JSON performance...")
        for i in range(iterations):
            try:
                start_time = time.time()
                _ = flow(
                    from_date=from_date,
                    to_date=to_date,
                    model=model,
                    model_code=model_code,
                    file_format='json'
                )
                end_time = time.time()
                json_times.append(end_time - start_time)
                print(f"JSON Iteration {i+1}/{iterations} completed in {end_time - start_time:.2f} seconds")
            except Exception as e:
                print(f"Error in JSON iteration {i+1}: {str(e)}")
                continue
    
    # Test Parquet performance if file exists
    if parquet_files:
        print("\nTesting Parquet performance...")
        for i in range(iterations):
            try:
                start_time = time.time()
                _ = flow(
                    from_date=from_date,
                    to_date=to_date,
                    model=model,
                    model_code=model_code,
                    file_format='parquet'
                )
                end_time = time.time()
                parquet_times.append(end_time - start_time)
                print(f"Parquet Iteration {i+1}/{iterations} completed in {end_time - start_time:.2f} seconds")
            except Exception as e:
                print(f"Error in Parquet iteration {i+1}: {str(e)}")
                continue
    
    # Calculate results only if we have successful iterations
    results = {}
    if json_times:
        results['json'] = {
            'mean': sum(json_times) / len(json_times),
            'min': min(json_times),
            'max': max(json_times),
            'std': pd.Series(json_times).std() if len(json_times) > 1 else 0
        }
        print("\nJSON Performance:")
        print(f"Mean time: {results['json']['mean']:.3f} seconds")
        print(f"Min time: {results['json']['min']:.3f} seconds")
        print(f"Max time: {results['json']['max']:.3f} seconds")
        print(f"Std dev: {results['json']['std']:.3f} seconds")
    
    if parquet_times:
        results['parquet'] = {
            'mean': sum(parquet_times) / len(parquet_times),
            'min': min(parquet_times),
            'max': max(parquet_times),
            'std': pd.Series(parquet_times).std() if len(parquet_times) > 1 else 0
        }
        print("\nParquet Performance:")
        print(f"Mean time: {results['parquet']['mean']:.3f} seconds")
        print(f"Min time: {results['parquet']['min']:.3f} seconds")
        print(f"Max time: {results['parquet']['max']:.3f} seconds")
        print(f"Std dev: {results['parquet']['std']:.3f} seconds")
    
    if json_times and parquet_times:
        json_mean = results['json']['mean']
        parquet_mean = results['parquet']['mean']
        improvement = ((json_mean - parquet_mean) / json_mean) * 100
        
        print("\nPerformance Comparison:")
        print("-" * 50)
        print(f"JSON average time:    {json_mean:.3f} seconds")
        print(f"Parquet average time: {parquet_mean:.3f} seconds")
        print(f"Speed difference:     {abs(json_mean - parquet_mean):.3f} seconds")
        if improvement > 0:
            print(f"Parquet is {improvement:.1f}% faster than JSON")
            print(f"Parquet processes data {json_mean/parquet_mean:.1f}x faster than JSON")
        else:
            print(f"JSON is {-improvement:.1f}% faster than Parquet")
            print(f"JSON processes data {parquet_mean/json_mean:.1f}x faster than Parquet")
    
    if not results:
        raise ValueError("No successful iterations completed for either format")
    
    return results

# Example usage
if __name__ == "__main__":
    try:
        results = compare_flow_performance(
            from_date="2023-09-01 00:00:00",
            to_date="2023-09-08 00:00:00",
            model='city',
            model_code='',
            iterations=5
        )
    except Exception as e:
        print(f"Error running performance test: {str(e)}")