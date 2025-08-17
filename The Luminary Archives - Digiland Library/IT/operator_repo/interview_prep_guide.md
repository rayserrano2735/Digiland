# Data Engineering Interview Prep Guide

> Your personal cheat sheet mapping interview questions to repository patterns

## Table of Contents
1. [SQL Questions](#sql-questions)
2. [Data Modeling Questions](#data-modeling-questions)
3. [ETL/Pipeline Questions](#etlpipeline-questions)
4. [Performance & Optimization](#performance--optimization)
5. [Data Quality & Testing](#data-quality--testing)
6. [Modern Stack Questions](#modern-stack-questions)
7. [System Design Questions](#system-design-questions)
8. [Behavioral Questions with Technical Examples](#behavioral-questions)

---

## SQL Questions

### Q: "How would you find duplicate records in a table?"
**Pattern**: [Data Quality Checks](./sql-patterns/data-quality-checks/)
```sql
-- Show this code from data_quality_checks.sql
WITH duplicates AS (
    SELECT customer_id, email, COUNT(*) as duplicate_count
    FROM customers
    GROUP BY customer_id, email
    HAVING COUNT(*) > 1
)
SELECT * FROM duplicates;
```
**Talk about**: NULL handling, performance with large datasets, composite keys

### Q: "Explain window functions and when to use them"
**Pattern**: [Window Functions](./sql-patterns/window-functions/)
```sql
-- Perfect example: Running totals without self-joins
SELECT date, amount,
    SUM(amount) OVER (ORDER BY date) as running_total,
    AVG(amount) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7day
FROM transactions;
```
**Key points**: 
- More efficient than self-joins
- Doesn't collapse rows like GROUP BY
- Essential for time-series analysis

### Q: "How do you compare two tables to find differences?"
**Pattern**: [Table Comparison](./sql-patterns/table-comparison/)
- Show the complete solution with column mapping
- Mention the Python tool for handling different schemas
- Discuss NULL handling complexities

### Q: "Convert rows to columns (pivot) in SQL"
**Pattern**: [Pivot/Unpivot](./sql-patterns/pivot-unpivot/)
```sql
-- Universal approach works in all databases
SELECT product,
    SUM(CASE WHEN quarter = 'Q1' THEN revenue END) AS Q1,
    SUM(CASE WHEN quarter = 'Q2' THEN revenue END) AS Q2
FROM sales GROUP BY product;
```

### Q: "How do you handle NULL values in SQL?"
**Multiple Patterns**:
- [Data Quality Checks](./sql-patterns/data-quality-checks/) - Detection
- [Table Comparison](./sql-patterns/table-comparison/) - NULL-safe comparisons
```sql
-- Key insight: NULL != NULL returns NULL, not FALSE
WHERE col1 != col2 
   OR (col1 IS NULL AND col2 IS NOT NULL)
   OR (col1 IS NOT NULL AND col2 IS NULL)
```

### Q: "Write a query to find gaps in sequential data"
**Pattern**: [Window Functions](./sql-patterns/window-functions/)
```sql
-- Gap and island problem
WITH numbered AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn
    FROM sequence_table
)
SELECT id + 1 as gap_start, 
       LEAD(id) - 1 as gap_end
FROM numbered
WHERE id + 1 != LEAD(id) OVER (ORDER BY id);
```

---

## Data Modeling Questions

### Q: "How do you handle slowly changing dimensions?"
**Pattern**: [Data Modeling Patterns](./data-modeling-patterns/slowly-changing-dimensions/)
```sql
-- Type 2 SCD with effective dates
SELECT customer_id, customer_name, address,
       effective_date, 
       COALESCE(end_date, '9999-12-31') as end_date,
       CASE WHEN end_date IS NULL THEN 1 ELSE 0 END as is_current
FROM customer_dimension;
```

### Q: "Design a star schema for sales analytics"
**Approach**:
1. Identify facts (sales amount, quantity)
2. Identify dimensions (date, product, customer, store)
3. Design surrogate keys
4. Show the [DBT macro](./dbt-patterns/macros/) for generating surrogate keys

### Q: "Explain when to use normalized vs denormalized models"
**Answer Framework**:
- OLTP → Normalized (reduce redundancy)
- OLAP → Denormalized (query performance)
- Show [pivot pattern](./sql-patterns/pivot-unpivot/) as denormalization example

---

## ETL/Pipeline Questions

### Q: "How do you ensure data quality in pipelines?"
**Pattern**: [Data Quality Checks](./sql-patterns/data-quality-checks/)
```sql
-- Show the comprehensive quality report view
CREATE VIEW data_quality_report AS
-- Completeness, Uniqueness, Validity, Consistency checks
```
**Discuss**:
- Where to place checks (source, staging, final)
- Fail vs warn strategies
- Monitoring and alerting

### Q: "How do you handle incremental loads?"
**Patterns**: 
- [DBT Macros](./dbt-patterns/macros/) - `incremental_filter` macro
- [Window Functions](./sql-patterns/window-functions/) - Deduplication
```sql
-- DBT incremental pattern
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### Q: "How do you handle late-arriving data?"
**Approach**:
1. Window functions for deduplication
2. Merge/upsert strategies
3. Watermarking in streaming

### Q: "Design an idempotent data pipeline"
**Key Patterns**:
- Use [surrogate keys](./dbt-patterns/macros/) 
- MERGE instead of INSERT
- Transaction boundaries
- Exactly-once semantics

---

## Performance & Optimization

### Q: "How do you optimize a slow SQL query?"
**Systematic Approach**:
1. EXPLAIN plan analysis
2. Check indexes
3. Partition pruning
4. [Window functions](./sql-patterns/window-functions/) instead of self-joins
5. [Broadcast joins](./spark-patterns/optimizations/) for Spark

### Q: "How do you handle data skew in distributed systems?"
**Pattern**: [Spark Optimizations](./spark-patterns/optimizations/)
```python
# Salting technique
def salted_join(left_df, right_df, join_key, salt_range=10):
    # Add random salt to distribute skewed keys
```

### Q: "Optimize Spark job performance"
**Pattern**: [Spark Optimizations](./spark-patterns/optimizations/)
- Broadcast joins for small tables
- Partition optimization (128MB target)
- Caching strategy
- Predicate pushdown

### Q: "When to use columnar vs row storage?"
**Answer**:
- Columnar (Parquet): Analytics, aggregations, few columns
- Row (Avro): Transactional, full record access
- Show compression benefits in columnar

---

## Data Quality & Testing

### Q: "How do you test data pipelines?"
**Multi-Layer Approach**:
1. Unit tests - [Python patterns](./python-patterns/testing/)
2. Data quality checks - [SQL patterns](./sql-patterns/data-quality-checks/)
3. DBT tests - [DBT macros](./dbt-patterns/macros/)
```yaml
# DBT schema test
models:
  - name: orders
    tests:
      - not_null_percentage:
          threshold: 0.95
```

### Q: "How do you monitor data freshness?"
**Pattern**: [Data Quality Checks](./sql-patterns/data-quality-checks/)
```sql
SELECT MAX(updated_at) as last_update,
       CURRENT_TIMESTAMP - MAX(updated_at) as staleness
FROM source_table;
```

### Q: "Handle outliers in data"
**Pattern**: [Data Quality Checks](./sql-patterns/data-quality-checks/)
- Z-score method
- IQR method
- Business rule validation

---

## Modern Stack Questions

### Q: "Explain DBT and its benefits"
**Pattern**: [DBT Patterns](./dbt-patterns/)
- Show macros for reusability
- Demonstrate testing framework
- Version control for SQL
- Documentation as code

### Q: "How do you implement CI/CD for data?"
**Components**:
1. DBT tests in CI
2. Data quality checks
3. Schema change detection
4. Automated documentation

### Q: "Orchestration tool experience"
**Patterns**: [Airflow Patterns](./airflow-patterns/)
- Dynamic DAG generation
- Sensor patterns
- Error handling
- Backfill strategies

---

## System Design Questions

### Q: "Design a real-time analytics system"
**Architecture Components**:
1. Ingestion: Kafka patterns
2. Processing: Spark streaming
3. Storage: Lambda architecture
4. Serving: Aggregation patterns

### Q: "Design a data warehouse from scratch"
**Approach**:
1. Requirements gathering
2. [Data modeling](./data-modeling-patterns/)
3. ETL design with [quality checks](./sql-patterns/data-quality-checks/)
4. Performance optimization
5. Security and governance

### Q: "How do you handle PII data?"
**Strategies**:
- Encryption at rest/transit
- Column-level security
- Masking/tokenization
- Audit logging
- GDPR compliance

---

## Behavioral Questions

### Q: "Tell me about a challenging data problem you solved"
**Use the STAR Method + Code**:
- **Situation**: Data migration validation needed
- **Task**: Ensure 100% accuracy across 50M records
- **Action**: Built [table comparison pattern](./sql-patterns/table-comparison/)
- **Result**: Found 0.01% discrepancies, prevented bad reports
- **Show the actual code**

### Q: "How do you handle conflicting priorities?"
**Technical Example**:
"I built this [repository structure](./tools/) to automate repetitive tasks, saving 5 hours/week for strategic work"

### Q: "Describe a time you improved performance"
**Multiple Examples**:
1. [Window functions](./sql-patterns/window-functions/) - 10x speedup
2. [Spark optimizations](./spark-patterns/optimizations/) - Reduced runtime 50%
3. [Broadcast joins](./spark-patterns/optimizations/) - Eliminated shuffles

### Q: "How do you ensure code quality?"
**Show Your Repository**:
- Organized structure
- Comprehensive documentation
- Reusable patterns
- Testing strategies
- Version control

---

## Interview Day Checklist

### Before the Interview
- [ ] Review this guide
- [ ] Have repository open on laptop
- [ ] Test all code examples
- [ ] Prepare 2-3 stories using STAR method

### During the Interview
- [ ] Listen for keywords that map to patterns
- [ ] Mention you have production code examples
- [ ] Share screen if virtual interview
- [ ] Explain the "why" not just "how"

### Key Phrases to Use
- "I have a production-ready pattern for this..."
- "Let me show you how I handle this in practice..."
- "I've optimized this for performance by..."
- "The edge cases to consider are..."
- "In my experience, the gotchas are..."

### Questions to Ask Them
1. "What are your biggest data quality challenges?"
2. "How do you handle schema evolution?"
3. "What's your approach to testing data pipelines?"
4. "How do you balance normalization vs performance?"

---

## Pattern Quick Reference

| Problem | Pattern | File |
|---------|---------|------|
| Find duplicates | Data Quality | `data_quality_checks.sql` |
| Compare tables | Table Comparison | `compare_tables_dynamic.sql` |
| Running totals | Window Functions | `window_functions.sql` |
| Pivot data | Pivot/Unpivot | `pivot_examples.sql` |
| Data freshness | Data Quality | `data_quality_checks.sql` |
| Incremental loads | DBT Macros | `dbt_macros.sql` |
| Spark optimization | Spark Patterns | `spark_optimizations.py` |
| Column mapping | Table Comparison | `column_mapper.py` |

---

## Remember

1. **You built tools** to maintain this repository - that's senior-level thinking
2. **Your patterns handle edge cases** - NULLs, data types, performance
3. **Everything is production-tested** - not just theoretical
4. **You can explain the why** - not just copy-paste solutions

**Your repository demonstrates**:
- Technical depth
- Organizational skills
- Documentation ability
- Problem-solving approach
- Production mindset

*You're not just a data engineer - you're a patterns library curator and automation builder.*