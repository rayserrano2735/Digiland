# Filename: python-patterns/sql-equivalents/window_functions.py
"""
Window Functions in Pandas - SQL to Python Translation
For when they ask: "Now do it in Python"
"""

import pandas as pd
import numpy as np

# =====================================================
# THE BIG THREE RANKING FUNCTIONS
# =====================================================

def ranking_functions_demo():
    """
    ROW_NUMBER, RANK, DENSE_RANK in pandas
    """
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Carol', 'David', 'Eve'],
        'score': [95, 92, 92, 90, 88]
    })
    
    # ROW_NUMBER() - unique sequential number
    df['row_number'] = df['score'].rank(method='first', ascending=False).astype(int)
    
    # RANK() - same rank for ties, gaps after
    df['rank'] = df['score'].rank(method='min', ascending=False).astype(int)
    
    # DENSE_RANK() - same rank for ties, no gaps
    df['dense_rank'] = df['score'].rank(method='dense', ascending=False).astype(int)
    
    return df

# =====================================================
# QUESTION 1: Deduplication (MOST COMMON!)
# =====================================================

def remove_duplicates_window_style(df, partition_cols, order_col):
    """
    SQL: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC)
    
    Example:
        df = load_movie_updates()
        deduped = remove_duplicates_window_style(
            df, 
            partition_cols=['movie_id'],
            order_col='updated_at'
        )
    """
    # Sort by partition columns and order column
    df_sorted = df.sort_values(
        partition_cols + [order_col], 
        ascending=[True]*len(partition_cols) + [False]
    )
    
    # Add row number within each group
    df_sorted['rn'] = df_sorted.groupby(partition_cols).cumcount() + 1
    
    # Keep only first row per group
    return df_sorted[df_sorted['rn'] == 1].drop('rn', axis=1)

# =====================================================
# QUESTION 2: Top N per Group
# =====================================================

def top_n_per_group(df, group_col, value_col, n=3):
    """
    SQL: ROW_NUMBER() OVER (PARTITION BY group ORDER BY value DESC)
    
    Example:
        top_movies = top_n_per_group(
            movies_df,
            group_col='genre',
            value_col='rating',
            n=3
        )
    """
    return (df.groupby(group_col)
              .apply(lambda x: x.nlargest(n, value_col))
              .reset_index(drop=True))

# =====================================================
# QUESTION 3: Running Totals / Cumulative Sum
# =====================================================

def calculate_running_totals(df, date_col, value_col, group_col=None):
    """
    SQL: SUM() OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    """
    df_sorted = df.sort_values(date_col)
    
    if group_col:
        # Running total per group
        df_sorted['running_total'] = (df_sorted.groupby(group_col)[value_col]
                                               .cumsum())
    else:
        # Overall running total
        df_sorted['running_total'] = df_sorted[value_col].cumsum()
    
    return df_sorted

# =====================================================
# QUESTION 4: Moving Averages
# =====================================================

def moving_average(df, date_col, value_col, window_size=7):
    """
    SQL: AVG() OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    """
    df_sorted = df.sort_values(date_col)
    
    # Calculate moving average
    df_sorted[f'ma_{window_size}'] = (
        df_sorted[value_col]
        .rolling(window=window_size, min_periods=1)
        .mean()
    )
    
    return df_sorted

# =====================================================
# QUESTION 5: LAG / LEAD Operations
# =====================================================

def lag_lead_operations(df, partition_col, order_col, value_col):
    """
    SQL: LAG(value) OVER (PARTITION BY ... ORDER BY ...)
         LEAD(value) OVER (PARTITION BY ... ORDER BY ...)
    """
    df_sorted = df.sort_values([partition_col, order_col])
    
    # LAG - previous value
    df_sorted[f'{value_col}_prev'] = (
        df_sorted.groupby(partition_col)[value_col].shift(1)
    )
    
    # LEAD - next value
    df_sorted[f'{value_col}_next'] = (
        df_sorted.groupby(partition_col)[value_col].shift(-1)
    )
    
    # Calculate change from previous
    df_sorted[f'{value_col}_change'] = (
        df_sorted[value_col] - df_sorted[f'{value_col}_prev']
    )
    
    return df_sorted

# =====================================================
# THE COMPLETE PATTERN: Real Interview Example
# =====================================================

def analyze_movie_ratings(movies_df):
    """
    Complete example using IMDB data - what you'd show in interview
    """
    # Add multiple window functions in one go
    movies_df = movies_df.sort_values(['year', 'rating'], ascending=[True, False])
    
    # Rankings per year
    movies_df['year_rank'] = (movies_df.groupby('year')['rating']
                                       .rank(method='first', ascending=False)
                                       .astype(int))
    
    # Overall dense rank
    movies_df['overall_rank'] = (movies_df['rating']
                                          .rank(method='dense', ascending=False)
                                          .astype(int))
    
    # Percentile rank
    movies_df['percentile'] = movies_df['rating'].rank(pct=True)
    
    # Year average (like a window aggregate)
    movies_df['year_avg'] = movies_df.groupby('year')['rating'].transform('mean')
    
    # Rating vs year average
    movies_df['vs_year_avg'] = movies_df['rating'] - movies_df['year_avg']
    
    # Cumulative votes for top movies in each year
    movies_df['cumulative_votes'] = (movies_df.groupby('year')['votes']
                                              .cumsum())
    
    # Get top 5 per year
    top_per_year = movies_df[movies_df['year_rank'] <= 5]
    
    return top_per_year

# =====================================================
# QUICK TEST HARNESS
# =====================================================

if __name__ == "__main__":
    # Test ranking functions
    print("Ranking Functions Demo:")
    print(ranking_functions_demo())
    print("\n" + "="*50 + "\n")
    
    # Test deduplication
    test_df = pd.DataFrame({
        'movie_id': [1, 1, 2, 2, 3],
        'rating': [8.5, 8.7, 7.2, 7.5, 9.0],
        'updated_at': pd.date_range('2024-01-01', periods=5)
    })
    
    print("Deduplication Demo:")
    print(remove_duplicates_window_style(
        test_df, 
        ['movie_id'], 
        'updated_at'
    ))

# =====================================================
# INTERVIEW TALKING POINTS
# =====================================================

"""
When they ask "Do this in Python":

1. ALWAYS mention: "In production, I'd do this in SQL for performance"
2. Show you know pandas: "Here's the pandas equivalent using rank() and transform()"
3. Discuss tradeoffs: "Pandas loads everything into memory, SQL processes on server"
4. Demonstrate testing: "Let me verify this matches the SQL output"

COMMON GOTCHAS:
- pandas rank() has different methods ('first', 'min', 'dense')
- groupby().cumsum() for running totals within groups
- shift() for LAG/LEAD operations
- transform() to add group aggregates without reducing rows

YOUR POWER PHRASE:
"I typically handle this in SQL for performance, but here's the pandas
equivalent. Notice how I'm using transform() to avoid the merge..."
"""