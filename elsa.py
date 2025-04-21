
None selected 

Skip to content
Using Gmail with screen readers

Conversations
me
(no subject)
 
Attachment:
new 32.txt
3:59 PM
12.73 GB of 15 GB (84%) used
Terms · Privacy · Program Policies
Last account activity: 2 minutes ago
Details
# Import necessary libraries
import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, BooleanType, MapType, ArrayType
from pyspark.sql.functions import col, pandas_udf, PandasUDFType

# List of packages to install
packages = ['pandas', 'numpy', 'google-cloud-storage']
for package in packages:
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

import pandas as pd
import numpy as np
import warnings
from collections import defaultdict
import math
from pyspark.sql.utils import AnalysisException
import traceback
import json
from pyspark.sql.functions import countDistinct, count

warnings.filterwarnings("ignore")  # Suppress warnings for cleaner output

# --- Constants ---
print("\nLet's define few constants:")
PRICE_SMOOTHING_THRESHOLD = 0.05  # 5% threshold for considering prices the same
PRICE_SPIKE_THRESHOLD = 0.50  # 50% threshold for imputing large spikes
IQR_MULTIPLIER = 1.5  # For outlier detection
MIN_WEEKS_FOR_BLOCK = 1  # Minimum weeks needed to consider a block valid
ROUNDING_DECIMALS = 2
EPSILON = 1e-9  # Small constant to avoid log(0) or division by zero
print("\nConstant values are defined, going forward with execution")

# --- Helper Functions ---
print("\nExecuting safe_log...")
def safe_log(numerator, denominator):
    """Calculates log(numerator / denominator) safely."""
    num = max(float(numerator), EPSILON)
    den = max(float(denominator), EPSILON)

    if den == 0 or math.isnan(num) or math.isnan(den):
        return np.nan

    ratio = num / den
    if ratio <= 0:
        return np.nan

    result = np.log(ratio)
    return result
  
print("\nExecuting the IQR Bounds...")    
def calculate_iqr_bounds(series):
    """Calculates IQR bounds for a pandas Series, handling small samples."""
    if series.empty or series.isnull().all():
        return -np.inf, np.inf

    non_null_count = series.dropna().count()
    if non_null_count < 4:  # Need at least 4 points for robust IQR
        min_val, max_val = series.min(), series.max()
        return min_val - EPSILON, max_val + EPSILON  # Use min/max if too few points

    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1

    # Handle cases where IQR is zero (all values in the middle are the same)
    if iqr == 0:
        median_val = series.median()
        return median_val - EPSILON, median_val + EPSILON

    lower_bound = q1 - IQR_MULTIPLIER * iqr
    upper_bound = q3 + IQR_MULTIPLIER * iqr
    return lower_bound, upper_bound
    
print("\nExecuting the Block Stats...")   
def calculate_block_stats(df_block):
    """Calculates average units and price for a block after IQR outlier removal."""
    if df_block.empty:
        return np.nan, np.nan, 0, 0  # Added original count

    original_count = len(df_block)
    units_series = df_block['TOTAL_UNITS'].astype(float)  # Ensure float for quantile

    # Remove unit outliers using IQR
    lower_bound, upper_bound = calculate_iqr_bounds(units_series)
    
    # Be careful with bounds that might become infinite
    lower_bound = lower_bound if np.isfinite(lower_bound) else -np.inf
    upper_bound = upper_bound if np.isfinite(upper_bound) else np.inf

    df_filtered = df_block[(units_series >= lower_bound) & (units_series <= upper_bound)].copy()
    num_weeks_after_filter = len(df_filtered)

    if df_filtered.empty:
        # If filtering removed everything, return NaN but report original count
        return np.nan, np.nan, 0, original_count

    avg_units = df_filtered['TOTAL_UNITS'].mean()
    # Use RETAIL_AMT for price calculation as it's imputed earlier
    avg_price = df_filtered['RETAIL_AMT'].mean()

    # Ensure results are not NaN/inf if possible
    avg_units = avg_units if pd.notna(avg_units) and np.isfinite(avg_units) else np.nan
    avg_price = avg_price if pd.notna(avg_price) and np.isfinite(avg_price) else np.nan

    return avg_units, avg_price, num_weeks_after_filter, original_count

print("\nExecuting the Elasticities Calculation...")

# Define a schema for the output of our pandas_udf
result_schema = StructType([
    StructField("UPC_NBR", StringType(), True),
    StructField("STORE_NBR", StringType(), True),
    StructField("key", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("weeks", IntegerType(), True)
])

# Define a function to process each group in pandas format
@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def calculate_elasticities_formulas_udf(df_group):
    """
    Calculates elasticities and multipliers for a single UPC/Store group
    using the formula-based approach, adapted for PySpark pandas UDF.
    """
    # Ensure df_group is sorted by week
    df_group = df_group.sort_values('WM_YR_WK').reset_index(drop=True)
    
    group_results = defaultdict(list)
    processed_blocks = []  # To store details of consecutive blocks

    # 1. Pre-process: Impute RETAIL_AMT and identify promo type
    df_group['RETAIL_AMT'] = df_group['RETAIL_AMT'].fillna(df_group['REG_RETAIL_AMT'])
    
    # Ensure RETAIL_AMT reflects REG_RETAIL_AMT when not on INSTORE_PROMO
    df_group['RETAIL_AMT'] = np.where(df_group['INSTORE_PROMO'] == 0,
                                     df_group['REG_RETAIL_AMT'],
                                     df_group['RETAIL_AMT'])
                                     
    # Ensure prices are numeric
    df_group['RETAIL_AMT'] = pd.to_numeric(df_group['RETAIL_AMT'], errors='coerce')
    df_group['REG_RETAIL_AMT'] = pd.to_numeric(df_group['REG_RETAIL_AMT'], errors='coerce')
    df_group = df_group.dropna(subset=['RETAIL_AMT', 'REG_RETAIL_AMT'])  # Remove rows where prices couldn't be numeric

    conditions = [
        (df_group['INSTORE_PROMO'] == 0),
        (df_group['INSTORE_PROMO'] == 1) & (df_group['FLYER'] == 0) & (df_group['CPP'] == 0),
        (df_group['INSTORE_PROMO'] == 1) & (df_group['FLYER'] == 1) & (df_group['CPP'] == 0),
        (df_group['INSTORE_PROMO'] == 1) & (df_group['FLYER'] == 0) & (df_group['CPP'] == 1),
        (df_group['INSTORE_PROMO'] == 1) & (df_group['FLYER'] == 1) & (df_group['CPP'] == 1)
    ]
    choices = ['No_Promo', 'Instore_Only', 'Instore_Flyer', 'Instore_CPP', 'Instore_Flyer_CPP']
    df_group['Promo_Type'] = np.select(conditions, choices, default='Other_Promo')  # Handle unexpected combinations

    # 2. Identify Consecutive Blocks (Refined)
    group_shift = df_group[['Promo_Type', 'REG_RETAIL_AMT']].shift(1)
    block_change = (df_group['Promo_Type'] != group_shift['Promo_Type']) | \
                  ((df_group['Promo_Type'] == 'No_Promo') & (df_group['REG_RETAIL_AMT'] != group_shift['REG_RETAIL_AMT']))
    df_group['Block_ID'] = block_change.cumsum()

    current_reg_price = np.nan

    # Process blocks first to apply smoothing/spiking and get reliable stats
    block_data_for_calc = {}
    for block_id in df_group['Block_ID'].unique():
        block_df = df_group[df_group['Block_ID'] == block_id].copy()  # Use copy to avoid SettingWithCopyWarning
        if block_df.empty:
            continue

        block_promo_type = block_df['Promo_Type'].iloc[0]
        start_wk = block_df['WM_YR_WK'].min()
        end_wk = block_df['WM_YR_WK'].max()
        # Use median for representative price to be less sensitive to intermediate fluctuations within a block
        block_reg_price = block_df['REG_RETAIL_AMT'].median()

        # Handle Regular Price Smoothing and Spike Imputation (only for No_Promo blocks)
        if block_promo_type == 'No_Promo':
            if pd.notna(current_reg_price) and pd.notna(block_reg_price) and current_reg_price > EPSILON:
                price_diff_ratio = abs(block_reg_price - current_reg_price) / current_reg_price
                # Check for spike > 50%
                if price_diff_ratio > PRICE_SPIKE_THRESHOLD:
                    block_df['REG_RETAIL_AMT'] = current_reg_price
                    block_df['RETAIL_AMT'] = current_reg_price  # Impute retail too for no promo
                    block_reg_price = current_reg_price  # Update the representative price for this block
                # Check for smoothing <= 5%
                elif price_diff_ratio <= PRICE_SMOOTHING_THRESHOLD:
                    block_reg_price = current_reg_price  # Treat price as unchanged for comparison purposes

            # Update current_reg_price only if it's a valid No_Promo block and price is not NaN
            if pd.notna(block_reg_price):
                current_reg_price = block_reg_price  # Use the potentially smoothed/imputed price

        # Calculate stats after potential imputation/smoothing
        avg_units, avg_price, num_weeks_filtered, original_weeks = calculate_block_stats(block_df)

        # Store detailed data for calculations
        block_data_for_calc[block_id] = {
            'block_id': block_id,
            'promo_type': block_promo_type,
            'start_wk': start_wk,
            'end_wk': end_wk,
            'reg_price_processed': block_reg_price,  # The representative price after smoothing/spiking checks
            'avg_price_filtered': avg_price,  # Average retail price after unit outlier removal
            'avg_units_filtered': avg_units,  # Average units after unit outlier removal
            'num_weeks_filtered': num_weeks_filtered,  # Weeks used in stat calculation
            'original_weeks': original_weeks  # Original weeks in block
        }

    # Convert processed data back to a list sorted by block_id for easier iteration
    processed_blocks = [block_data_for_calc[bid] for bid in sorted(block_data_for_calc.keys())]

    # --- Calculations using Processed Blocks ---
    # 3. Calculate Regular Price Elasticity (E_reg)
    e_reg_results = []
    for i in range(1, len(processed_blocks)):  # Start from the second block
        old_block = processed_blocks[i-1]
        new_block = processed_blocks[i]

        # Check for consecutive No_Promo blocks where the *processed* reg_price differs
        if new_block['promo_type'] == 'No_Promo' and old_block['promo_type'] == 'No_Promo' and \
           pd.notna(old_block['reg_price_processed']) and pd.notna(new_block['reg_price_processed']) and \
           abs(new_block['reg_price_processed'] - old_block['reg_price_processed']) > EPSILON:

            # Check if blocks have enough data and valid stats
            if old_block['num_weeks_filtered'] >= MIN_WEEKS_FOR_BLOCK and new_block['num_weeks_filtered'] >= MIN_WEEKS_FOR_BLOCK and \
               pd.notna(old_block['avg_units_filtered']) and pd.notna(new_block['avg_units_filtered']):

                log_price_change = safe_log(new_block['reg_price_processed'], old_block['reg_price_processed'])
                log_unit_change = safe_log(new_block['avg_units_filtered'], old_block['avg_units_filtered'])

                if pd.notna(log_price_change) and pd.notna(log_unit_change) and abs(log_price_change) > EPSILON:
                    e_reg = log_unit_change / log_price_change
                    # Outlier Check: Elasticity must be negative
                    if e_reg < 0:
                        e_reg_results.append({
                            'value': round(e_reg, ROUNDING_DECIMALS),
                            # Report original weeks before filtering for context
                            'weeks': old_block['original_weeks'] + new_block['original_weeks'],
                            'details': f"From P={old_block['reg_price_processed']:.2f}({old_block['original_weeks']}w)@U={old_block['avg_units_filtered']:.2f} "
                                      f"To P={new_block['reg_price_processed']:.2f}({new_block['original_weeks']}w)@U={new_block['avg_units_filtered']:.2f}"
                        })

    group_results['E_regular'] = e_reg_results

    # 4. Calculate Promo Elasticity (E_promo)
    e_promo_results = []
    promo_elasticity_map = {}  # Store calculated E_promo keyed by the reference reg block id

    for i in range(len(processed_blocks)):
        current_block = processed_blocks[i]
        if current_block['promo_type'] == 'Instore_Only':  # Calculate E_promo for Instore_Only blocks

            # Find the immediately preceding No_Promo block
            last_reg_block = None
            for j in range(i - 1, -1, -1):
                if processed_blocks[j]['promo_type'] == 'No_Promo':
                    last_reg_block = processed_blocks[j]
                    break

            if last_reg_block and \
               current_block['num_weeks_filtered'] >= MIN_WEEKS_FOR_BLOCK and last_reg_block['num_weeks_filtered'] >= MIN_WEEKS_FOR_BLOCK and \
               pd.notna(current_block['avg_units_filtered']) and pd.notna(last_reg_block['avg_units_filtered']) and \
               pd.notna(current_block['avg_price_filtered']) and pd.notna(last_reg_block['reg_price_processed']):  # Use processed reg price

                log_price_change = safe_log(current_block['avg_price_filtered'], last_reg_block['reg_price_processed'])
                log_unit_change = safe_log(current_block['avg_units_filtered'], last_reg_block['avg_units_filtered'])

                if pd.notna(log_price_change) and pd.notna(log_unit_change) and abs(log_price_change) > EPSILON:
                    e_promo = log_unit_change / log_price_change
                    # Outlier Check: Elasticity must be negative
                    if e_promo < 0:
                        e_promo_results.append({
                            'value': round(e_promo, ROUNDING_DECIMALS),
                            'weeks': current_block['original_weeks'] + last_reg_block['original_weeks'],
                            'details': f"Promo P={current_block['avg_price_filtered']:.2f}({current_block['original_weeks']}w)@U={current_block['avg_units_filtered']:.2f} "
                                      f"vs Base P={last_reg_block['reg_price_processed']:.2f}({last_reg_block['original_weeks']}w)@U={last_reg_block['avg_units_filtered']:.2f}"
                        })
                        # Store the calculated E_promo based on the reference block ID
                        promo_elasticity_map[last_reg_block['block_id']] = e_promo

    group_results['E_promo'] = e_promo_results

    # 5. Calculate Multipliers (M)
    # Note: These use the E_promo calculated relative to the preceding No_Promo block
    multiplier_results = defaultdict(list)
    multiplier_map = {
        'Instore_Flyer': 'M_instore_flyer',
        'Instore_CPP': 'M_instore_cpp',
        'Instore_Flyer_CPP': 'M_instore_flyer_cpp'
    }

    for i in range(len(processed_blocks)):
        current_block = processed_blocks[i]

        if current_block['promo_type'] in multiplier_map:
            promo_key = multiplier_map[current_block['promo_type']]

            # Find the immediately preceding No_Promo block
            last_reg_block = None
            for j in range(i - 1, -1, -1):
                if processed_blocks[j]['promo_type'] == 'No_Promo':
                    last_reg_block = processed_blocks[j]
                    break

            # Check if we have the corresponding E_promo needed
            if last_reg_block and last_reg_block['block_id'] in promo_elasticity_map:
                e_promo_ref = promo_elasticity_map[last_reg_block['block_id']]  # Get relevant E_promo

                # Check if blocks have enough data and valid stats
                if current_block['num_weeks_filtered'] >= MIN_WEEKS_FOR_BLOCK and last_reg_block['num_weeks_filtered'] >= MIN_WEEKS_FOR_BLOCK and \
                   pd.notna(current_block['avg_units_filtered']) and pd.notna(last_reg_block['avg_units_filtered']) and \
                   pd.notna(current_block['avg_price_filtered']) and pd.notna(last_reg_block['reg_price_processed']) and \
                   pd.notna(e_promo_ref):  # Ensure E_promo is valid

                    lift = max(current_block['avg_units_filtered'], EPSILON) / max(last_reg_block['avg_units_filtered'], EPSILON)
                    price_ratio = max(current_block['avg_price_filtered'], EPSILON) / max(last_reg_block['reg_price_processed'], EPSILON)

                    if price_ratio > 0 and pd.notna(lift):
                        try:
                            denominator = price_ratio ** e_promo_ref
                            if abs(denominator) > EPSILON:  # Avoid division by zero/very small numbers
                                raw_multiplier = lift / denominator

                                # Apply Governor: Multiplier must be >= 1.00
                                final_multiplier = max(1.0, raw_multiplier)

                                multiplier_results[promo_key].append({
                                    'value': round(final_multiplier, ROUNDING_DECIMALS),
                                    'raw_value': round(raw_multiplier, ROUNDING_DECIMALS),  # Store raw value for reference
                                    'weeks': current_block['original_weeks'] + last_reg_block['original_weeks'],
                                    'details': f"Promo P={current_block['avg_price_filtered']:.2f}({current_block['original_weeks']}w)@U={current_block['avg_units_filtered']:.2f} "
                                             f"vs Base P={last_reg_block['reg_price_processed']:.2f}({last_reg_block['original_weeks']}w)@U={last_reg_block['avg_units_filtered']:.2f} "
                                             f"using E_promo={e_promo_ref:.2f}"
                                })
                        except (OverflowError, ValueError):
                            pass

    group_results.update(multiplier_results)  # Add multiplier results

    # Format results into tabular form for return
    result_rows = []
    upc_nbr = df_group['UPC_NBR'].iloc[0]
    store_nbr = df_group['STORE_NBR'].iloc[0]
    
    # Extract and flatten the results
    for metric_type, results in group_results.items():
        if not results:
            continue
            
        for idx, item in enumerate(results):
            key = f"{metric_type}_{idx+1}" if len(results) > 1 else metric_type
            row = {
                "UPC_NBR": upc_nbr,
                "STORE_NBR": store_nbr,
                "key": key,
                "value": float(item['value']),
                "weeks": int(item['weeks'])
            }
            result_rows.append(row)
    
    if not result_rows:
        # Return an empty DataFrame with the expected schema
        return pd.DataFrame(columns=["UPC_NBR", "STORE_NBR", "key", "value", "weeks"])
    
    return pd.DataFrame(result_rows)


# Main execution
if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("UPC Elasticity Analysis") \
        .getOrCreate()
    
    file_path = 'gs://01f9b3d68098e5580b130a147137b1b9667bd0a2598fdf70911038d08ab2a4/samarth/001_Price_Elasticity/elasticitydata.csv'  # Use the provided file path

    try:
        # Read CSV into Spark DataFrame
        df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
        df_raw.printSchema()
        df_raw.show()
        df_raw.groupBy("UPC_NBR") \
            .agg(
                countDistinct("WM_YR_WK").alias("Weeks"),
                countDistinct("STORE_NBR").alias("Stores"),
                count("*").alias("TotalRecords")
            ) \
            .orderBy("UPC_NBR").show()
        
        print(f"Loaded data: {df_raw.count()} rows")

        # Basic Cleaning
        df_raw = df_raw.dropna(subset=['WM_YR_WK', 'UPC_NBR', 'STORE_NBR', 'REG_RETAIL_AMT', 'TOTAL_UNITS'])
        
        # Convert columns to numeric and drop any resulting nulls
        df_raw = df_raw.withColumn('TOTAL_SALES', F.col('TOTAL_SALES').cast('double'))
        df_raw = df_raw.withColumn('TOTAL_UNITS', F.col('TOTAL_UNITS').cast('double'))
        df_raw = df_raw.dropna(subset=['TOTAL_SALES', 'TOTAL_UNITS'])

        # Filter out negative units/sales BEFORE grouping
        df_cleaned = df_raw.filter((F.col('TOTAL_SALES') >= 0) & (F.col('TOTAL_UNITS') >= 0))
        print(f"Data after initial cleaning (negatives removed): {df_cleaned.count()} rows")

        if df_cleaned.count() == 0:
            print("No valid data after initial cleaning.")
        else:
            # Ensure grouping columns are correct type
            df_cleaned = df_cleaned.withColumn('UPC_NBR', F.col('UPC_NBR').cast('string'))
            df_cleaned = df_cleaned.withColumn('STORE_NBR', F.col('STORE_NBR').cast('string'))
            
            # Apply the pandas UDF to each group
            results_df = df_cleaned.groupBy('UPC_NBR', 'STORE_NBR').apply(calculate_elasticities_formulas_udf)
            
            # Check if results are empty
            if results_df.count() == 0:
                print("No metrics calculated for any group.")
            else:
                # Collect results for printing
                results_collected = results_df.collect()
                
                print("\n--- Final Calculated Metrics ---")
                
                # Process results for output formatting
                all_results = {}
                for row in results_collected:
                    key = (row['UPC_NBR'], row['STORE_NBR'])
                    if key not in all_results:
                        all_results[key] = {}
                    
                    metric_key = row['key']
                    all_results[key][metric_key] = {
                        'value': row['value'],
                        'weeks': row['weeks']
                    }
                
                # Print results in formatted output
                for (upc, store), metrics in all_results.items():
                    print(f"\nUPC: {upc}, Store: {store}")
                    output_str_parts = []
                    
                    # Define the order for printing keys based on prefixes
                    key_prefixes_ordered = ['E_regular', 'E_promo', 'M_instore_flyer', 'M_instore_cpp', 'M_instore_flyer_cpp']
                    processed_keys = set()

                    for prefix in key_prefixes_ordered:
                        # Find all keys starting with the prefix
                        keys_for_prefix = sorted([k for k in metrics if k.startswith(prefix)],
                                               key=lambda x: int(x.split('_')[-1]) if '_' in x and x.split('_')[-1].isdigit() else 0)

                        if keys_for_prefix:
                            prefix_results = []
                            for key in keys_for_prefix:
                                value = metrics[key]['value']
                                weeks = metrics[key]['weeks']
                                prefix_results.append(f"{key}: {value} ({weeks} weeks)")
                                processed_keys.add(key)
                            output_str_parts.append(', '.join(prefix_results))

                    # Add any remaining keys not covered by the ordered prefixes
                    remaining_keys = sorted(list(set(metrics.keys()) - processed_keys))
                    if remaining_keys:
                        remaining_results = []
                        for key in remaining_keys:
                            value = metrics[key]['value']
                            weeks = metrics[key]['weeks']
                            remaining_results.append(f"{key}: {value} ({weeks} weeks)")
                        output_str_parts.append(', '.join(remaining_results))

                    print('; '.join(part for part in output_str_parts if part))

    except Exception as e:
        print(f"An unexpected error occurred during execution: {e}")
        traceback.print_exc()
        
    finally:
        # Stop Spark session
        spark.stop()
