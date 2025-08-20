# Azure Data Engineering Quick Reference

## Azure Data Factory (ADF)

### Core Components
```json
Pipeline → Activities → Datasets → Linked Services → Integration Runtime
```

### Activity Types
- **Copy Activity**: Move data between stores
- **Data Flow**: Visual ETL transformations
- **Databricks Notebook**: Your strength!
- **Stored Procedure**: SQL execution
- **Web Activity**: REST API calls
- **Lookup**: Get data for dynamic pipelines
- **ForEach/If/Until**: Control flow

### Key Expressions
```python
# Parameters
@pipeline().parameters.tableName
@dataset().path

# System Variables  
@pipeline().RunId
@pipeline().TriggerTime
@trigger().startTime

# Activity Outputs
@activity('LookupActivity').output.firstRow
@activity('GetMetadata').output.childItems

# Functions
@concat('folder/', formatDateTime(utcnow(), 'yyyy-MM-dd'))
@substring(item().name, 0, 10)
@coalesce(pipeline().parameters.inputPath, 'default/path')
```

### Copy Activity Pattern
```json
{
  "name": "CopyFromBlobToSQL",
  "type": "Copy",
  "inputs": [{"referenceName": "BlobDataset"}],
  "outputs": [{"referenceName": "SqlDataset"}],
  "typeProperties": {
    "source": {
      "type": "BlobSource",
      "recursive": true
    },
    "sink": {
      "type": "SqlSink",
      "writeBatchSize": 10000
    },
    "enableStaging": true
  }
}
```

### Dynamic Pipeline Pattern
```json
// Lookup + ForEach
1. Lookup Activity → Get table list
2. ForEach Activity → @activity('Lookup').output.value
3. Inside ForEach → Copy Activity with @item().tableName
```

### Triggers
- **Schedule**: Cron-like scheduling
- **Tumbling Window**: Fixed-size, non-overlapping time windows
- **Event**: Blob creation/deletion
- **Manual**: On-demand

## Azure Stream Analytics

### Query Structure
```sql
WITH TempData AS (
    SELECT 
        deviceId,
        temperature,
        EventEnqueuedUtcTime as eventTime
    FROM input
)
SELECT 
    System.Timestamp() as WindowEnd,
    deviceId,
    AVG(temperature) as AvgTemp,
    MIN(temperature) as MinTemp,
    MAX(temperature) as MaxTemp,
    COUNT(*) as EventCount
INTO output
FROM TempData TIMESTAMP BY eventTime
GROUP BY 
    deviceId, 
    TumblingWindow(minute, 5)
HAVING AVG(temperature) > 75
```

### Window Functions
```sql
-- Tumbling: Fixed, non-overlapping
TumblingWindow(second/minute/hour, size)

-- Hopping: Fixed, overlapping  
HoppingWindow(timeunit, windowsize, hopsize)

-- Sliding: Triggers on events
SlidingWindow(timeunit, size)

-- Session: Variable size, gap-based
SessionWindow(timeunit, timeout, maxduration)
```

### Common Patterns
```sql
-- Late Arrival Handling
SELECT * FROM input 
TIMESTAMP BY EventTime 
WITHIN 5 minutes

-- Reference Data Join
SELECT 
    stream.deviceId,
    stream.temperature,
    ref.deviceName,
    ref.location
FROM stream
JOIN ref ON stream.deviceId = ref.deviceId

-- Anomaly Detection
SELECT 
    deviceId,
    temperature,
    AnomalyDetection_SpikeAndDip(temperature, 95, 120, 'spikesanddips')
    OVER(LIMIT DURATION(second, 120)) as scores
FROM input
```

### Output Patterns
```sql
-- Multiple Outputs
SELECT * INTO sqlOutput FROM stream WHERE temperature > 100
SELECT * INTO blobArchive FROM stream
SELECT * INTO alertOutput FROM stream WHERE temperature > 120

-- Partitioned Output
SELECT * INTO output 
PARTITION BY deviceType
FROM input
```

## Integration Patterns

### ADF → Databricks → Synapse
```python
1. ADF Lookup: Get processing parameters
2. ADF Databricks Activity: 
   - Notebook path: /Shared/ProcessData
   - Base parameters: {"date": "@trigger().startTime"}
3. ADF Copy: Move results to Synapse
```

### Event-Driven Pipeline
```python
Event Hub → Stream Analytics → ADF Trigger → Databricks → Data Lake
```

### Monitoring & Alerts
- ADF: Monitor runs, set alerts on failure
- Stream Analytics: Track input/output events, watermark delays
- Both integrate with Azure Monitor/Log Analytics

## Common Interview Questions

**Q: ADF vs Databricks for ETL?**
A: ADF for orchestration and simple transforms, Databricks for complex processing

**Q: How handle late arriving data in streams?**
A: TIMESTAMP BY with tolerance window, watermarking strategies

**Q: ADF pricing optimization?**
A: Use TTL on Integration Runtime, batch small files, optimize Data Flow partitions

**Q: Stream Analytics vs Databricks Streaming?**
A: Stream Analytics for simple SQL-based streaming, Databricks for complex ML/joins