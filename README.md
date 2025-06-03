# DataProbe: Large Dataset Analysis Toolkit

## Description

DataProbe is a Python toolkit designed for efficient analysis and manipulation of individual columns within large datasets that may not fit into memory. It leverages the Dask library for out-of-core computation, enabling operations like data sampling, intelligent data type detection, synthetic data generation, and comprehensive statistical analysis on potentially massive CSV or Parquet files.

The primary script, `src/dataprobe.py`, provides a suite of functions to help understand and work with large columnar data.

## Features

*   **Efficient Data Sampling**: Sample a specified number of data points from any column of a large DataFrame without loading the entire file into memory.
*   **Intelligent Data Type Detection**: Automatically infer the substantive data type of a column (e.g., distinguishing between floats that are actually integers, or strings that represent numbers).
*   **Synthetic Data Generation**: Create realistic fake data based on the statistical properties (mean, standard deviation for numerical; discrete distribution for categorical/other) of a sample from the original column.
*   **Comprehensive Column Statistics**: Calculate a wide range of statistics for a column, including counts (total, NA, unique), numerical summaries (min, max, mean, median, std), boolean counts, date ranges, and string frequency analysis.
*   **Large Dataset Handling**: Utilizes Dask to perform computations on datasets larger than available RAM.
*   **Flexible Data Source Input**: Works with Dask DataFrames directly or file paths (CSV, Parquet with appropriate `read_function_kwargs`).


## Setup

It is highly recommended to use uv to manage dependencies.

`uv` is a fast Python package installer and resolver, written in Rust. It can be significantly faster than `pip` and `venv`.

1.  **Install `uv`** (if you haven't already):
2. **Install Dependencies**
```shell
git clone git@github.com:AvinasiLabsDeveloper/delong-data-pipeline.git
cd delong-data-pipeline
uv sync
```

## Usage

The core functionalities are provided by the functions within `src/dataprobe.py`.

### Example Workflow:

```python
from src.dataprobe import (
    sample_column_from_large_dataframe,
    get_intelligent_data_type,
    fake_data,
    stat_column_data
)

# Define your data source and column
csv_file_path = 'path/to/your/large_datafile.csv'
target_column_name = 'your_column_of_interest'

# 1. Sample data from the column
# (Using default dd.read_csv, pass read_function_kwargs if needed for other formats or options)
try:
    sampled_data = sample_column_from_large_dataframe(
        data_source=csv_file_path,
        column_name=target_column_name,
        n_samples=10000,
        random_state=42
    )
    print(f"Successfully sampled {len(sampled_data)} data points from '{target_column_name}'.")

    # 2. Determine the intelligent data type of the sampled data
    inferred_type = get_intelligent_data_type(sampled_data)
    print(f"Inferred data type for '{target_column_name}': {inferred_type}")

    # 3. Generate fake data based on the sample
    generated_fakes = fake_data(column_data=sampled_data, n_samples=500)
    print(f"Generated {len(generated_fakes)} fake data points. First 5: \n{generated_fakes.head()}")

    # 4. Get comprehensive statistics for the entire column from the source
    # This will process the whole column in the CSV using Dask
    column_stats = stat_column_data(
        data_source=csv_file_path,
        column_name=target_column_name
    )
    print(f"\nFull statistics for column '{target_column_name}':")
    for key, value in column_stats.items():
        print(f"  {key}: {value}")

except Exception as e:
    print(f"An error occurred: {e}")

```

## Key Functions Overview

### 1. `sample_column_from_large_dataframe`
   - **Description**: Samples a specified number of data points from a single column of a large DataFrame (Dask-backed).
   - **Parameters**:
     - `data_source (Union[str, dd.DataFrame])`: Path to the data file or a Dask DataFrame.
     - `column_name (str)`: Name of the column to sample from.
     - `n_samples (int)`: Number of data points to sample.
     - `random_state (Optional[int])`: Seed for reproducible sampling.
     - `read_function_kwargs (Optional[Dict[str, Any]])`: Arguments for Dask's read functions (e.g., `usecols` for `dd.read_csv`, `columns` for `dd.read_parquet`).
   - **Returns**: `pd.Series` containing the sampled data.

### 2. `get_intelligent_data_type`
   - **Description**: Infers the most representative "intelligent" data type of a Pandas Series (e.g., float `1.0` becomes `int`, string `"123"` becomes `int`).
   - **Parameters**:
     - `column_data (pd.Series)`: The input data series (typically a sample).
   - **Returns**: `str` representing the inferred type (e.g., `'int'`, `'float'`, `'bool'`, `'datetime'`, `'str'`).

### 3. `fake_data`
   - **Description**: Generates a new Pandas Series of synthetic data based on the statistical properties of an input sample series.
   - **Parameters**:
     - `column_data (pd.Series)`: A sample series to model the fake data upon.
     - `n_samples (int)`: Number of fake data points to generate.
   - **Returns**: `pd.Series` of generated fake data.
     - Numerical data is generated from a normal distribution (mean, std of sample).
     - Categorical/other data is generated based on the discrete probability distribution of the sample.

### 4. `stat_column_data`
   - **Description**: Computes comprehensive statistics for an entire column in a large dataset using Dask. Type inference is performed on a head sample.
   - **Parameters**:
     - `data_source (Union[str, dd.DataFrame])`: Path to the data file or a Dask DataFrame.
     - `column_name (str)`: Name of the column for statistical analysis.
     - `read_function_kwargs (Optional[Dict[str, Any]])`: Arguments for Dask read functions.
     - `type_inference_sample_size (int)`: Number of rows from the head of the column used for type inference (default: 1000).
   - **Returns**: `Dict[str, Any]` containing various statistics like counts, NA percentage, min/max, mean, median, std, unique counts, top frequent values, etc., appropriate to the inferred data type.

## Notes on Large Datasets

This toolkit is built with large datasets in mind. By using Dask, operations are performed in a memory-efficient way, processing data in chunks. This allows analysis of files that are too large to be loaded into RAM entirely with standard Pandas. When providing file paths, ensure Dask has the necessary means to read them (e.g., for CSVs, Parquet files). The `read_function_kwargs` parameter is crucial for optimizing reads, especially by specifying only the `column_name` in `usecols` (for CSV) or `columns` (for Parquet) to avoid reading unnecessary data.
