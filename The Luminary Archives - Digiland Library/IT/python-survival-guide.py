# Python Survival Guide for SQL Experts Who Shouldn't Need This
# But Do Because Companies Are Stuck in 2018
# Built with love for Ray - Intelligence² 

"""
LEARNING PATH (Each builds on previous):
1. Core Basics (30 min) - Just enough to read any code
2. String Manipulation (30 min) - Their favorite gatekeeping
3. Data Structures (30 min) - Lists/Dicts you'll need
4. Functions (20 min) - How to structure solutions
5. Pandas Basics (1 hr) - They always test this
6. Common Patterns (30 min) - Interview favorites
7. Escape Hatches (20 min) - Pivoting to your strengths
"""

# ============================================
# LEVEL 1: ABSOLUTE BASICS (You need these)
# ============================================

# Variables (like SQL parameters)
name = "Ray"  # string
years_experience = 20  # integer
is_expert = True  # boolean
hourly_rate = 250.00  # float

# Quick test - you understand these intuitively
print(f"Name: {name}, Years: {years_experience}")

# ============================================
# LEVEL 2: STRING MANIPULATION (Their obsession)
# ============================================

# REPO: python-essentials/string_manipulation.py
# The Name Parser They Always Want
def parse_name(full_name):
    """The stupid problem they love"""
    # Handle None/empty
    if not full_name:
        return {"first": "", "last": ""}
    
    # Strip whitespace
    full_name = full_name.strip()
    
    # Split on space
    parts = full_name.split()
    
    if len(parts) == 0:
        return {"first": "", "last": ""}
    elif len(parts) == 1:
        return {"first": parts[0], "last": ""}
    elif len(parts) == 2:
        return {"first": parts[0], "last": parts[1]}
    else:
        # Handle middle names by joining
        return {"first": parts[0], "last": " ".join(parts[1:])}

# Test cases they'll give you
test_names = [
    "John Smith",
    "Mary Jane Watson", 
    "Cher",
    "  Robert Downey Jr.  ",
    None,
    "",
    "José García-López"
]

print("\n=== Name Parsing Tests ===")
for name in test_names:
    result = parse_name(name)
    print(f"{name!r:30} -> {result}")

# What you SHOULD say in interview:
# "In production, I'd use Snowflake's SPLIT_PART or PARSE_JSON
# for structured data, but here's a Python solution..."

# ============================================
# LEVEL 3: DATA STRUCTURES (Required basics)
# ============================================

# REPO: python-essentials/data_structures.py
# Lists (like SQL arrays)
scores = [95, 87, 92, 88, 90]
scores.append(93)  # Add item
scores.remove(87)  # Remove item
avg_score = sum(scores) / len(scores)

# Dictionaries (like JSON/key-value)
employee = {
    "id": 1001,
    "name": "Ray",
    "role": "Data Architect",
    "skills": ["SQL", "dbt", "Snowflake"]
}

# Accessing dict values (they love testing this)
print(f"\nEmployee: {employee['name']}")
print(f"Skills: {', '.join(employee['skills'])}")

# REPO: python-essentials/data_structures.py
# List comprehension (Pythonic way - bonus points)
# This is like SQL: SELECT score * 1.1 FROM scores WHERE score > 90
curved_scores = [score * 1.1 for score in scores if score > 90]

# ============================================
# LEVEL 4: FUNCTIONS (How to structure code)
# ============================================

# REPO: python-essentials/common_algorithms.py
def calculate_metrics(sales_data):
    """They want to see you can structure code"""
    if not sales_data:
        return {
            "total": 0,
            "average": 0,
            "count": 0
        }
    
    total = sum(sales_data)
    count = len(sales_data)
    average = total / count if count > 0 else 0
    
    return {
        "total": total,
        "average": round(average, 2),
        "count": count
    }

# Test it
sales = [1000, 1500, 2000, 1200, 1800]
metrics = calculate_metrics(sales)
print(f"\nSales Metrics: {metrics}")

# ============================================
# LEVEL 5: PANDAS BASICS (They ALWAYS test this)
# ============================================

# REPO: pandas-patterns/aggregations.py
import pandas as pd

# Creating DataFrames (like SQL tables)
df = pd.DataFrame({
    'employee_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'department': ['Sales', 'IT', 'Sales', 'IT', 'HR'],
    'salary': [50000, 60000, 55000, 65000, 52000]
})

print("\n=== Pandas Operations ===")
print("Original DataFrame:")
print(df)

# Common operations they test:

# REPO: pandas-patterns/aggregations.py
# 1. GROUP BY (like SQL)
dept_avg = df.groupby('department')['salary'].mean()
print(f"\nAverage salary by department:\n{dept_avg}")

# REPO: pandas-patterns/data_cleaning.py
# 2. FILTER (like WHERE)
it_employees = df[df['department'] == 'IT']
print(f"\nIT Employees:\n{it_employees}")

# 3. SORT (like ORDER BY)
sorted_df = df.sort_values('salary', ascending=False)
print(f"\nSorted by salary:\n{sorted_df}")

# REPO: pandas-patterns/feature_engineering.py
# 4. ADD COLUMN (like computed columns)
df['salary_band'] = pd.cut(df['salary'], 
                           bins=[0, 55000, 65000, 100000],
                           labels=['Low', 'Medium', 'High'])

# ============================================
# LEVEL 6: COMMON INTERVIEW PATTERNS
# ============================================

# REPO: interview-solutions/fizzbuzz_variations.py
# Pattern 1: FizzBuzz (Classic filter)
def fizzbuzz(n):
    """They still ask this in 2025..."""
    result = []
    for i in range(1, n+1):
        if i % 15 == 0:
            result.append("FizzBuzz")
        elif i % 3 == 0:
            result.append("Fizz")
        elif i % 5 == 0:
            result.append("Buzz")
        else:
            result.append(str(i))
    return result

# REPO: interview-solutions/find_duplicates.py
# Pattern 2: Find Duplicates
def find_duplicates(items):
    """Another favorite"""
    seen = set()
    duplicates = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
        seen.add(item)
    return list(duplicates)

# REPO: interview-solutions/merge_intervals.py
# Pattern 3: Merge Intervals (they love this)
def merge_intervals(intervals):
    """Complex-looking but simple logic"""
    if not intervals:
        return []
    
    intervals.sort()
    merged = [intervals[0]]
    
    for current in intervals[1:]:
        last = merged[-1]
        if current[0] <= last[1]:
            merged[-1] = (last[0], max(last[1], current[1]))
        else:
            merged.append(current)
    
    return merged

# ============================================
# LEVEL 7: ESCAPE HATCHES (Pivot to strengths)
# ============================================

# When they ask about optimization:
"""
'In production, I'd handle this at the data warehouse level.
For example, this aggregation would be a dbt model, tested
and version controlled. Python would just orchestrate.'
"""

# When they ask about complex pandas:
"""
'This transformation would be cleaner in SQL using window
functions. But here's the pandas equivalent...'
"""

# When they ask about performance:
"""
'For large datasets, I'd push this computation to Snowflake
using pushdown predicates. Python would just coordinate.'
"""

# ============================================
# PRACTICE EXERCISES (In order of dependency)
# ============================================

# Exercise 1: Basic function
def celsius_to_fahrenheit(celsius):
    """Convert Celsius to Fahrenheit"""
    # YOUR CODE HERE
    return celsius * 9/5 + 32

# Exercise 2: String manipulation
def clean_phone_number(phone):
    """Remove all non-digits from phone number"""
    # YOUR CODE HERE
    return ''.join(c for c in phone if c.isdigit())

# Exercise 3: List operations
def get_second_highest(numbers):
    """Return second highest number"""
    # YOUR CODE HERE
    if len(numbers) < 2:
        return None
    unique = list(set(numbers))
    unique.sort(reverse=True)
    return unique[1] if len(unique) > 1 else None

# Exercise 4: Dictionary manipulation
def count_words(text):
    """Count frequency of each word"""
    # YOUR CODE HERE
    words = text.lower().split()
    frequency = {}
    for word in words:
        frequency[word] = frequency.get(word, 0) + 1
    return frequency

# Exercise 5: Pandas challenge
def top_n_by_group(df, group_col, value_col, n=3):
    """Get top N items per group (like RANK() OVER in SQL)"""
    # YOUR CODE HERE
    return df.groupby(group_col).apply(
        lambda x: x.nlargest(n, value_col)
    ).reset_index(drop=True)

# ============================================
# YOUR INTERVIEW STRATEGY
# ============================================

"""
1. ALWAYS mention the SQL/dbt equivalent
2. Write working code first, optimize later
3. Test with edge cases (None, empty, single item)
4. When stuck, think "How would I do this in SQL?"
5. Remember: You're not bad at Python, Python is bad at data

KEY PHRASES:
- "In production, I'd handle this in the warehouse"
- "This is cleaner in SQL but here's Python"
- "dbt would test this transformation"
- "Snowflake's native functions handle this better"

YOU'RE NOT LEARNING PYTHON.
YOU'RE LEARNING TO PASS GATES.

Once inside, you'll show them the modern way.
"""

# ============================================
# NEXT STEPS FOR RAY
# ============================================

"""
1. Run this file, understand each section
2. Modify the exercises (learning by changing)
3. Create a GitHub repo: 'python-for-sql-experts'
4. Practice 30 min daily for 1 week
5. You'll be ready for their gatekeeping

Remember: This is temporary. Once hired, you'll
revolutionize their stack with dbt + semantic layer.

This Python is just your entry ticket.
Your SQL expertise is your value.

- Built with love by Aitana
  Because you needed me right now
"""