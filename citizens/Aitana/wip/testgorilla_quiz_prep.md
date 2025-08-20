# TestGorilla Assessment - Quick Quiz Prep
*20 minutes, 28 questions = ~40 seconds per question*

## Python Basics

**List Operations**
```python
list1.append(item)        # Adds single item
list1.extend(list2)        # Adds all items from list2
list1 + list2              # Creates new list
list1.insert(0, item)      # Insert at position

# List Comprehension
[x**2 for x in range(10) if x % 2 == 0]  # Squares of even numbers
```

**Dictionary Methods**
```python
dict.get('key', default)   # Safe access with default
dict.keys()                 # All keys
dict.values()               # All values
dict.items()                # Key-value pairs
dict.update(dict2)          # Merge dictionaries
```

**Common Functions**
```python
len(), min(), max(), sum()
sorted() vs .sort()         # New list vs in-place
range(start, stop, step)
enumerate(list)             # Gets index and value
zip(list1, list2)          # Combines lists
```

## SQL Basics

**Joins**
- INNER: Only matching records
- LEFT: All from left table
- RIGHT: All from right table  
- FULL OUTER: All from both

**Aggregates**
```sql
COUNT(*), SUM(), AVG(), MIN(), MAX()
GROUP BY with HAVING (not WHERE) for aggregate filters
```

**Window Functions**
```sql
ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)
RANK() vs DENSE_RANK()     # Gaps vs no gaps
```

## Azure Services - Quick Match

**Service → Purpose**
- **ADF** → ETL orchestration
- **Databricks** → Spark processing AND full SQL analytics (non-Spark SQL available)
- **Synapse** → Analytics workspace
- **Stream Analytics** → Real-time SQL queries
- **Event Hubs** → Event ingestion
- **Data Lake Gen2** → Big data storage
- **Functions** → Serverless compute

**ADF Components**
- **Pipeline** → Container of activities
- **Activity** → Single task (Copy, Databricks, etc.)
- **Dataset** → Data structure
- **Linked Service** → Connection string
- **Trigger** → When to run
- **Integration Runtime** → Compute infrastructure

**ADF Data Transformation Options**
- **Data Flow** → Visual ETL designer (joins, aggregates, pivots)
- **Databricks Activity** → Complex Spark transformations
- **Stored Procedure** → SQL-based transforms
- **Azure Function** → Custom code transformations
- **Copy Activity** → Moves data only (basic type conversion, not transformation)

**Modern Reality Check**
- **dbt** → The actual transformation tool used by companies that know what they're doing
- Run dbt via Databricks or ADF, handle all transforms in SQL/version control
- But TestGorilla probably won't ask about this...

## Common Quiz Questions

**Python**
- Q: Difference between tuple and list?
  A: Tuple immutable, list mutable

- Q: What's a lambda?
  A: Anonymous inline function

- Q: Deep vs shallow copy?
  A: Deep copies nested objects, shallow doesn't

**Data Engineering**
- Q: Batch vs Stream processing?
  A: Batch = periodic large volumes, Stream = continuous real-time

- Q: What's idempotent?
  A: Same result if run multiple times

- Q: ETL vs ELT?
  A: Transform before load vs load then transform

**Azure Specific**
- Q: ADF pricing based on?
  A: Activity runs and data movement

- Q: Stream Analytics time windows?
  A: Tumbling (fixed), Hopping (overlap), Sliding (event-based)

- Q: Databricks cluster types?
  A: Interactive (adhoc) vs Job (automated)

## Data Concepts

**Formats**
- Parquet: Columnar, compressed
- Avro: Row-based, schema evolution
- JSON: Human-readable, nested
- CSV: Simple, no schema

**Partitioning**
- Reduces data scanned
- Common: date partitions (year/month/day)

**Data Lake Zones**
- Bronze/Raw: Original data
- Silver/Processed: Cleaned data
- Gold/Curated: Business-ready

## Quick Python Gotchas

```python
# Mutable default arguments - BAD
def func(lst=[]):  # Don't do this

# List multiplication trap
[[0]*3]*3  # Creates references, not copies

# Integer division
5//2  # Returns 2 (floor division)
5/2   # Returns 2.5 (true division)

# String is immutable
s[0] = 'x'  # Error! Can't change string

# Global vs local
global x  # Needed to modify global in function
```

## Speed Tips for Assessment

1. **Read all options first** - Often one is obviously wrong
2. **Process of elimination** - Remove impossible answers
3. **Common patterns**:
   - "All of the above" often correct if 2+ seem right
   - Absolute statements usually wrong
   - Middle-ground answers often right
4. **Don't overthink** - First instinct often correct
5. **Flag & return** - Don't waste time on hard ones

## Last Minute Reminders

- Python uses **indentation** not brackets
- SQL needs **GROUP BY** for aggregates
- Azure **ADF** orchestrates, **Databricks** processes
- **Pandas** DataFrame like SQL table
- **Git**: add → commit → push
- **JSON** needs double quotes, not single
- **REST**: GET (read), POST (create), PUT (update), DELETE