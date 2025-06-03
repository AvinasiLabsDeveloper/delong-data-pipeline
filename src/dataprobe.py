import dask.dataframe as dd
import pandas as pd
from typing import Union, Optional, Dict, Any
import numpy as np
from pandas.api.types import (
    is_integer_dtype,
    is_float_dtype,
    is_object_dtype,
    is_string_dtype,
    is_categorical_dtype,
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_timedelta64_dtype
)


def sample_column_from_large_dataframe(
    data_source: Union[str, dd.DataFrame],
    column_name: str,
    n_samples: int,
    random_state: Optional[int] = None,
    read_function_kwargs: Optional[Dict[str, Any]] = None
) -> pd.Series:
    """
    Samples data from a specified column of a potentially very large DataFrame (processed via Dask).

    Args:
    data_source (Union[str, dd.DataFrame]): 
        The data source. Can be a Dask DataFrame object or a file path (e.g., 'data.csv', 'data.parquet').
    column_name (str): 
        The name of the column to sample from.
    n_samples (int): 
        The desired number of samples. If n_samples < 0, a ValueError will be raised.
        If n_samples == 0, returns an empty Pandas Series.
        If n_samples is greater than the total number of elements in the column, all elements are returned.
    random_state (Optional[int]): 
        Random seed for reproducible sampling. Defaults to None.
    read_function_kwargs (Optional[Dict[str, Any]]): 
        Additional arguments passed to the Dask read function (e.g., dd.read_csv) when data_source is a file path.
        Example: 
        - For CSV: {'blocksize': '64MB', 'usecols': [column_name, 'another_col_needed_by_filter_or_something']}
        - For Parquet: {'reader': dd.read_parquet, 'engine': 'pyarrow', 'columns': [column_name]}
        If 'reader' is not specified, defaults to dd.read_csv. Specifying only necessary columns via 'usecols' (CSV) 
        or 'columns' (Parquet) can significantly improve speed and reduce memory usage.

    Returns:
    pd.Series: A Pandas Series containing the sampled data.

    Note:
    - If data_source is a file path, the function defaults to trying to read with dd.read_csv.
      You can specify a different read function (e.g., dd.read_parquet) via the 'reader' key in read_function_kwargs.
    """
    if n_samples < 0:
        raise ValueError("Number of samples (n_samples) must be a non-negative integer.")

    ddf: dd.DataFrame
    if isinstance(data_source, str):
        # Copy kwargs to avoid modifying the original dictionary
        kwargs = read_function_kwargs.copy() if read_function_kwargs else {}
        reader_func = kwargs.pop('reader', dd.read_csv)
        
        try:
            ddf = reader_func(data_source, **kwargs)
        except Exception as e:
            raise ValueError(
                f"Failed to load Dask DataFrame from file path '{data_source}' using the specified reader and arguments. "
                f"Please check the file path, read function, and read_function_kwargs. Error: {e}"
            )
    elif isinstance(data_source, dd.DataFrame):
        ddf = data_source
    else:
        raise TypeError("data_source must be a file path string (str) or a Dask DataFrame object.")

    if column_name not in ddf.columns:
        # ddf.columns is usually metadata and does not trigger a large computation
        known_columns = list(ddf.columns)
        raise KeyError(f"Column '{column_name}' does not exist in the DataFrame. Available columns: {known_columns}")

    # Dask Series sample method
    # It samples 'n' items from the Series without replacement.
    # If n_samples is 0, it returns an empty Series.
    # If n_samples is greater than the number of items, it returns all items.
    tgt_col = ddf[column_name].dropna()
    total_len = len(tgt_col)
    if total_len > 0:   
        sampling_frac = min(n_samples / total_len, 1)
        sampled_series_dask = tgt_col.sample(frac=sampling_frac, random_state=random_state)
    else:
        print('Cannot sample as the target column is empty after dropna().')
        return pd.Series(dtype=ddf[column_name].dtype) # Return empty series with original dtype if possible
    
    # .compute() executes the Dask computation graph and returns a Pandas Series
    return sampled_series_dask.compute()


def get_intelligent_data_type(column_data: pd.Series) -> str:
    """
    Intelligently determines the data type of a Pandas Series.
    
    Args:
    column_data (pd.Series): The column data to be assessed.
    
    Returns:
    str: The actual data type. Includes int, float, bool, datetime, timedelta, str.
    """
    # Handle an entirely empty Series
    if column_data.empty:
        original_dtype = column_data.dtype
        if is_integer_dtype(original_dtype):
            return 'int'
        elif is_float_dtype(original_dtype):
            return 'float'
        elif is_bool_dtype(original_dtype):
            return 'bool'
        elif is_datetime64_any_dtype(original_dtype):
            return 'datetime'
        elif is_timedelta64_dtype(original_dtype):
            return 'timedelta'
        else: # object, string, etc.
            return 'str'

    # For type inference, remove NaN values
    # If the Series is all NaN, dropna() will result in an empty Series
    series_for_inference = column_data.dropna()

    # If it's empty after removing NaN (i.e., original Series was all NaN or empty itself)
    if series_for_inference.empty:
        original_dtype = column_data.dtype
        if is_integer_dtype(original_dtype):
            return 'int' # An all-NaN integer column is still type int
        elif is_float_dtype(original_dtype):
            return 'float' # An all-NaN float column is still type float
        elif is_bool_dtype(original_dtype):
            # Check for pandas BooleanArray with all NAs
            if hasattr(original_dtype, 'na_value') and pd.isna(original_dtype.na_value) and column_data.isnull().all():
                 return 'bool'
            return 'bool' if is_bool_dtype(original_dtype) else 'str' # General case
        elif is_datetime64_any_dtype(original_dtype):
            return 'datetime'
        elif is_timedelta64_dtype(original_dtype):
            return 'timedelta'
        else: # object, string, etc.
            return 'str'

    # 1. Boolean type check
    # Check original dtype for boolean, as dropna() might change Series's dtype if all values were NA
    if is_bool_dtype(column_data.dtype):
        return 'bool'

    # 2. Integer type check (original dtype)
    if is_integer_dtype(column_data.dtype):
        return 'int'

    # 3. Float type check (original dtype)
    if is_float_dtype(column_data.dtype):
        # series_for_inference is a non-empty float Series here
        # Check if all values are integers (e.g., 1.0, 2.0)
        if (series_for_inference % 1 == 0).all():
            return 'int'
        else:
            return 'float'

    # 4. Object/string/category type check
    if is_object_dtype(column_data.dtype) or \
       is_string_dtype(column_data.dtype) or \
       is_categorical_dtype(column_data.dtype):

        current_series_to_check = series_for_inference
        if is_categorical_dtype(series_for_inference.dtype):
            # Attempt to use categories if they are more specific, otherwise convert to string
            try:
                # If categories are already numeric, to_numeric will work well.
                # If categories are stringified numbers, astype(str) then to_numeric.
                if pd.api.types.is_numeric_dtype(series_for_inference.cat.categories.dtype):
                    current_series_to_check = series_for_inference # Use original series with numeric categories
                else: # Categories might be strings like "1", "2.0" or actual text
                    current_series_to_check = series_for_inference.astype(str)
            except AttributeError: # Should not happen if is_categorical_dtype is true
                 current_series_to_check = series_for_inference.astype(str)


        numeric_series = pd.to_numeric(current_series_to_check, errors='coerce')

        if not numeric_series.isnull().all():
            if not numeric_series.isnull().any(): 
                if (numeric_series % 1 == 0).all():
                    return 'int'
                else:
                    return 'float'
            else:
                # Mixed numeric strings and non-numeric strings (e.g., ["1", "text", "3.0"])
                return 'str'
        else:
            # All values could not be converted to numbers (e.g. ["apple", "banana"])
            # Or series_for_inference was empty (original column was object type and all NaNs)
            return 'str'
            
    # 5. Datetime type
    if is_datetime64_any_dtype(column_data.dtype):
        return 'datetime'
        
    # 6. Timedelta type
    if is_timedelta64_dtype(column_data.dtype):
        return 'timedelta'

    # Default fallback to 'str'
    return 'str'


def fake_data(column_data: pd.Series, n_samples: int) -> pd.Series:
    """
    Generates fake data based on the data type of the column.
    Receives the original sampled column data and the desired number of fake samples.
    For numerical data, assumes a normal distribution and estimates parameters for sampling.
    For other types, samples based on the discrete statistical distribution.

    Args:
    column_data (pd.Series): Column data sampled from the original data.
    n_samples (int): Desired number of fake data samples to generate.
    
    Returns:
    pd.Series: Generated fake data.
    """
    inferred_semantic_type = get_intelligent_data_type(column_data)

    if n_samples <= 0:
        if inferred_semantic_type == 'int': return pd.Series([], dtype=pd.Int64Dtype())
        elif inferred_semantic_type == 'float': return pd.Series([], dtype=float)
        elif inferred_semantic_type == 'bool': return pd.Series([], dtype=pd.BooleanDtype())
        elif inferred_semantic_type == 'datetime': return pd.Series([], dtype='datetime64[ns]')
        elif inferred_semantic_type == 'timedelta': return pd.Series([], dtype='timedelta64[ns]')
        else: return pd.Series([], dtype=object) # str

    valid_data_for_modeling = column_data.dropna()

    if valid_data_for_modeling.empty:
        # No actual values to model from (original was empty or all NAs).
        # Return a series of NAs of the inferred semantic type.
        if inferred_semantic_type == 'int': return pd.Series([pd.NA] * n_samples, dtype=pd.Int64Dtype())
        elif inferred_semantic_type == 'float': return pd.Series([np.nan] * n_samples, dtype=float)
        elif inferred_semantic_type == 'bool': return pd.Series([pd.NA] * n_samples, dtype=pd.BooleanDtype())
        elif inferred_semantic_type == 'datetime': return pd.Series([pd.NaT] * n_samples, dtype='datetime64[ns]')
        elif inferred_semantic_type == 'timedelta': return pd.Series([pd.NaT] * n_samples, dtype='timedelta64[ns]')
        else: return pd.Series([pd.NA] * n_samples, dtype=object) # str

    # At this point, valid_data_for_modeling is not empty.
    # And inferred_semantic_type is the guiding type for generation.

    if inferred_semantic_type == 'int' or inferred_semantic_type == 'float':
        numeric_data = pd.to_numeric(valid_data_for_modeling, errors='coerce')
        numeric_data = numeric_data.dropna() 

        if numeric_data.empty: 
            if inferred_semantic_type == 'int': return pd.Series([pd.NA] * n_samples, dtype=pd.Int64Dtype())
            return pd.Series([np.nan] * n_samples, dtype=float)

        mu = numeric_data.mean()
        sigma = numeric_data.std()
        
        if pd.isna(sigma) or sigma < 1e-9: 
            sigma = 0.0
        
        fake_samples_float = np.random.normal(mu, sigma, n_samples)

        if inferred_semantic_type == 'int':
            return pd.Series(np.rint(fake_samples_float), dtype=pd.Int64Dtype())
        else: # float
            return pd.Series(fake_samples_float, dtype=float)

    elif inferred_semantic_type == 'bool':
        probs = valid_data_for_modeling.value_counts(normalize=True)
        population = probs.index.tolist()
        weights = probs.values.tolist()
        
        fake_bools = np.random.choice(population, size=n_samples, p=weights)
        return pd.Series(fake_bools, dtype=pd.BooleanDtype())

    elif inferred_semantic_type in ['str', 'datetime', 'timedelta']:
        counts = valid_data_for_modeling.value_counts(normalize=True)
        population = counts.index
        weights = counts.values
        
        fake_values = np.random.choice(population, size=n_samples, p=weights)
        
        output_dtype = object
        if inferred_semantic_type == 'datetime':
            output_dtype = 'datetime64[ns]'
        elif inferred_semantic_type == 'timedelta':
            output_dtype = 'timedelta64[ns]'
        
        return pd.Series(fake_values, dtype=output_dtype)

    else: # Should not happen, defensive coding
        counts = valid_data_for_modeling.value_counts(normalize=True)
        population = counts.index
        weights = counts.values
        fake_values = np.random.choice(population, size=n_samples, p=weights)
        return pd.Series(fake_values, dtype=object)


def stat_column_data(
    data_source: Union[str, dd.DataFrame],
    column_name: str,
    read_function_kwargs: Optional[Dict[str, Any]] = None,
    type_inference_sample_size: int = 1000
) -> Dict[str, Any]:
    """
    Calculates statistics for a given column. First, infers the substantive data type
    based on a sample from the head of the column, then calculates full-column statistics,
    including min/max values, NA percentage, etc.

    Args:
    data_source (Union[str, dd.DataFrame]): The data source.
    column_name (str): The name of the column to be statistically analyzed.
    read_function_kwargs (Optional[Dict[str, Any]]): Dask read function arguments.
    type_inference_sample_size (int): Number of samples from the head for type inference.

    Returns:
    Dict[str, Any]: A dictionary containing the statistical information.
    """
    ddf_param: dd.DataFrame
    # 1. Load Dask DataFrame (or use provided one)
    if isinstance(data_source, str):
        kwargs = read_function_kwargs.copy() if read_function_kwargs else {}
        reader_func = kwargs.pop('reader', dd.read_csv)
        try:
            # Load the Dask DataFrame. If usecols/columns is in kwargs, it should be respected.
            # We select the specific column after loading to simplify logic around various reader params.
            temp_ddf = reader_func(data_source, **kwargs)
            if column_name not in temp_ddf.columns:
                 raise KeyError(f"Column '{column_name}' does not exist in DataFrame loaded from path '{data_source}'. Available columns: {list(temp_ddf.columns)}")
            ddf_param = temp_ddf[[column_name]] # Select only the column of interest early
        except Exception as e:
            raise ValueError(f"Failed to load Dask DataFrame from file path '{data_source}': {e}")
    elif isinstance(data_source, dd.DataFrame):
        if column_name not in data_source.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the provided Dask DataFrame. Available columns: {list(data_source.columns)}")
        ddf_param = data_source[[column_name]] # Select only the column of interest
    else:
        raise TypeError("data_source must be a file path string (str) or a Dask DataFrame object.")

    dask_series = ddf_param[column_name]

    # 2. Type Inference based on head sample
    head_sample_for_type = dask_series.head(n=type_inference_sample_size, compute=True)
    if not isinstance(head_sample_for_type, pd.Series):
        head_sample_for_type = pd.Series(head_sample_for_type, name=column_name) # Ensure it's a Series with a name

    substantive_type = get_intelligent_data_type(head_sample_for_type)
    
    stats: Dict[str, Any] = {
        "name": column_name,
        "determined_type": substantive_type,
        "original_dtype": str(dask_series.dtype)
    }

    # 3. Calculate Full Column Statistics using Dask operations
    
    # Basic counts - compute these first
    total_count_val = dask_series.size.compute() # Ensure it's computed if it's a Dask scalar
    na_count_val = dask_series.isnull().sum().compute()


    stats["total_count"] = int(total_count_val)
    stats["na_count"] = int(na_count_val)
    stats["na_percentage"] = (stats["na_count"] / stats["total_count"]) * 100 if stats["total_count"] > 0 else 0.0
    valid_count_val = stats["total_count"] - stats["na_count"]
    stats["valid_count"] = valid_count_val

    # Unique count - use nunique_approx for potentially very large series
    if valid_count_val > 0:
        # Heuristic: use nunique_approx if many valid items, otherwise exact.
        # nunique_approx is generally faster and uses less memory for high cardinality.
        if valid_count_val > 500_000 and dask_series.nunique_approx().compute() > 100_000 : # example threshold
            unique_count_dask = dask_series.nunique_approx()
        else:
            unique_count_dask = dask_series.nunique()
        stats["unique_count"] = unique_count_dask.compute()
    else:
        stats["unique_count"] = 0
        
    # Type-specific statistics
    stat_series = dask_series # Start with the original dask series

    if substantive_type in ['int', 'float']:
        if stat_series.dtype == 'object' or not pd.api.types.is_numeric_dtype(stat_series.dtype):
            meta_type = float 
            stat_series = stat_series.map_partitions(pd.to_numeric, errors='coerce', meta=(stat_series.name, meta_type))
        
        if valid_count_val > 0:
            # Run all dask computations together
            computed_numeric_stats = dd.compute(
                stat_series.min(),
                stat_series.max(),
                stat_series.mean(),
                stat_series.std(),
                stat_series.quantile(0.5) # median
            )
            min_s, max_s, mean_s, std_s, median_s = computed_numeric_stats
            
            stats["min"] = min_s if substantive_type == 'float' else (int(round(min_s)) if pd.notna(min_s) else pd.NA)
            stats["max"] = max_s if substantive_type == 'float' else (int(round(max_s)) if pd.notna(max_s) else pd.NA)
            stats["mean"] = mean_s # Keep float for mean, even if type is int
            stats["median"] = median_s if substantive_type == 'float' else (int(round(median_s)) if pd.notna(median_s) else pd.NA)
            stats["std"] = std_s if pd.notna(std_s) else 0.0
        else:
            stats.update({"min": pd.NA, "max": pd.NA, "mean": pd.NA, "median": pd.NA, "std": pd.NA})

    elif substantive_type == 'bool':
        if valid_count_val > 0:
            # value_counts() is safer for bool-like objects that might not be actual booleans yet
            value_counts_pd = stat_series.value_counts().compute() # pd.Series
            stats["true_count"] = int(value_counts_pd.get(True, 0))
            stats["false_count"] = int(value_counts_pd.get(False, 0))
            # Account for other values if not strictly bool due to object type before full conversion
            other_bool_values = {k:v for k,v in value_counts_pd.items() if k not in [True, False]}
            if other_bool_values:
                stats["other_bool_like_values_counts"] = other_bool_values
        else:
            stats["true_count"] = 0
            stats["false_count"] = 0

    elif substantive_type == 'datetime':
        if stat_series.dtype == 'object' or not pd.api.types.is_datetime64_any_dtype(stat_series.dtype):
            stat_series = stat_series.map_partitions(pd.to_datetime, errors='coerce', meta=(stat_series.name, 'datetime64[ns]'))
        
        if valid_count_val > 0:
            min_dt, max_dt = dd.compute(stat_series.min(), stat_series.max())
            stats.update({"min": min_dt, "max": max_dt})
        else:
            stats.update({"min": pd.NaT, "max": pd.NaT})

    elif substantive_type == 'timedelta':
        if stat_series.dtype == 'object' or not pd.api.types.is_timedelta64_dtype(stat_series.dtype):
             stat_series = stat_series.map_partitions(pd.to_timedelta, errors='coerce', meta=(stat_series.name, 'timedelta64[ns]'))
        if valid_count_val > 0:
            min_td, max_td = dd.compute(stat_series.min(), stat_series.max())
            stats.update({"min": min_td, "max": max_td})
        else:
            stats.update({"min": pd.NaT, "max": pd.NaT})

    elif substantive_type == 'str':
        if valid_count_val > 0:
            current_series_for_str_ops = stat_series
            # Ensure it's treated as string for value_counts if not already (e.g. categorical)
            if pd.api.types.is_categorical_dtype(current_series_for_str_ops.dtype):
                current_series_for_str_ops = current_series_for_str_ops.astype(str) # Dask astype(str)
            elif current_series_for_str_ops.dtype != 'object' and not pd.api.types.is_string_dtype(current_series_for_str_ops.dtype):
                 # If it's numeric/bool etc but classified as str (e.g. "1", "2"), convert to string for string ops
                current_series_for_str_ops = current_series_for_str_ops.astype(str)

            # Using .astype(str) above should make .min() and .max() lexicographical
            # and value_counts work on string representations.
            
            top_n_freq_dask = current_series_for_str_ops.value_counts().nlargest(5)
            # min/max on a series cast to string will be lexicographical
            min_lex_dask = current_series_for_str_ops.min() 
            max_lex_dask = current_series_for_str_ops.max()

            computed_str_stats = dd.compute(top_n_freq_dask, min_lex_dask, max_lex_dask)
            top_n_freq_val, min_lex_val, max_lex_val = computed_str_stats
            
            stats["top_5_frequent"] = top_n_freq_val.to_dict()
            stats["min_lexicographical"] = min_lex_val
            stats["max_lexicographical"] = max_lex_val
        else:
            stats["top_5_frequent"] = {}
            stats["min_lexicographical"] = pd.NA
            stats["max_lexicographical"] = pd.NA
            
    return stats


if __name__ == "__main__":
    csv_path = '/home/kido/avinasi/data_probe/dontdie-clockbase-agent/data/qced_human_dnam_age.csv'

    sample_data_points = sample_column_from_large_dataframe(csv_path, 'YingCausAge', 10000)
    generated_fake_data = fake_data(sample_data_points, 1000)
    column_statistics = stat_column_data(csv_path, 'YingCausAge')
    print("Column Statistics:")
    print(column_statistics)
    print("\nGenerated Fake Data (first 10 rows):")
    print(generated_fake_data.head(10))
    ...