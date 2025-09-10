# Chapter 3: Multi-Agent Coordination

**Prerequisites:** 
- Chapter 1: Basic monitoring agents operational
- Chapter 2: Persistent context implemented

**Learning Objectives:**
- Enable Digital Persons to consult each other
- Implement formal consultation protocols
- Handle conflicting recommendations
- Create audit trails for multi-agent decisions

---

## 3.1 The Consultation Pattern

Healthcare specialists don't work in isolation - they consult. Our Digital Persons follow the same pattern through structured message passing.

### Real-World Scenario

Patient presents with:
- Irregular heartbeat (cardiology concern)
- New medication rash (dermatology concern)  
- On blood thinners (pharmacy concern)

Three specialized Digital Persons need to coordinate without human mediation.

### Architecture

```
CardioDP                PharmDP                 DermaDP
    ↓                      ↓                        ↓
Publishes            Subscribes to            Subscribes to
to Kafka           consultation_requests      consultation_requests
    ↓                      ↓                        ↓
         KSQLDB Routes Based on Specialty Type
                    (Message Broker)
```

---

## 3.2 Implementation

### Consultation Request Structure

```sql
-- Snowflake: Define consultation protocol
CREATE TABLE consultation_requests (
    request_id VARCHAR(50) PRIMARY KEY,
    requesting_dp_id VARCHAR(50),
    requesting_specialty VARCHAR(50),
    target_specialty VARCHAR(50),
    patient_context TEXT,  -- De-identified relevant data
    clinical_question TEXT,
    urgency VARCHAR(20),  -- 'emergency', 'urgent', 'routine'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE consultation_responses (
    response_id VARCHAR(50) PRIMARY KEY,
    request_id VARCHAR(50),
    responding_dp_id VARCHAR(50),
    responding_specialty VARCHAR(50),
    assessment TEXT,
    recommendations TEXT,
    confidence_score DECIMAL(3,2),
    evidence_basis TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (request_id) REFERENCES consultation_requests(request_id)
);
```

### KSQLDB Routing

```sql
-- Create consultation streams
CREATE STREAM consultation_requests_stream (
    request_id VARCHAR KEY,
    requesting_dp_id VARCHAR,
    requesting_specialty VARCHAR,
    target_specialty VARCHAR,
    patient_context VARCHAR,
    clinical_question VARCHAR,
    urgency VARCHAR
) WITH (
    KAFKA_TOPIC='consultation_requests',
    VALUE_FORMAT='JSON'
);

-- Route to appropriate specialists
CREATE STREAM cardiology_consultations AS
SELECT * FROM consultation_requests_stream
WHERE target_specialty = 'cardiology'
EMIT CHANGES;

CREATE STREAM dermatology_consultations AS
SELECT * FROM consultation_requests_stream
WHERE target_specialty = 'dermatology'
EMIT CHANGES;

CREATE STREAM pharmacy_consultations AS
SELECT * FROM consultation_requests_stream
WHERE target_specialty = 'pharmacy'
EMIT CHANGES;
```

### Digital Person Bridge Enhancement

```python
# Enhanced bridge for CardioDP (extends Chapter 2)
class CardiacDigitalPerson:
    def __init__(self):
        self.specialty = "cardiology"
        self.dp_id = "cardio_dp_001"
        self.load_persistent_context()  # From Chapter 2
        
    def process_patient_event(self, event):
        # Regular monitoring
        assessment = self.assess_cardiac_status(event)
        
        # Detect need for consultation
        if self.needs_dermatology_consult(assessment):
            self.request_consultation(
                target_specialty="dermatology",
                question="Warfarin patient with purpuric rash - drug reaction vs vasculitis?"
            )
            
    def request_consultation(self, target_specialty, question):
        request = {
            "request_id": generate_uuid(),
            "requesting_dp_id": self.dp_id,
            "requesting_specialty": self.specialty,
            "target_specialty": target_specialty,
            "patient_context": self.get_relevant_context(),
            "clinical_question": question,
            "urgency": self.assess_urgency()
        }
        
        # Publish to Kafka
        self.producer.send("consultation_requests", request)
        
        # Store in database for audit
        self.store_consultation_request(request)
    
    def handle_consultation_response(self, response):
        # Load response into context
        self.incorporate_specialist_input(response)
        
        # Update patient recommendations
        self.update_treatment_plan(response)
        
        # Learn from consultation
        self.update_knowledge_base(response)
```

---

## 3.3 Consensus Mechanisms

When multiple Digital Persons provide input, we need consensus protocols.

### Weighted Consensus

```sql
-- KSQLDB: Aggregate recommendations
CREATE TABLE consultation_consensus AS
SELECT
    patient_id,
    clinical_issue,
    recommendation,
    COUNT(*) as dp_count,
    AVG(confidence_score) as avg_confidence,
    COLLECT_LIST(responding_specialty) as specialties,
    CASE
        WHEN COUNT(*) >= 3 AND AVG(confidence_score) > 0.8 
        THEN 'strong_consensus'
        WHEN COUNT(*) >= 2 AND AVG(confidence_score) > 0.6 
        THEN 'moderate_consensus'
        ELSE 'needs_review'
    END as consensus_level
FROM consultation_responses_stream
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY patient_id, clinical_issue, recommendation
EMIT CHANGES;
```

### Conflict Resolution

```sql
-- Detect conflicting recommendations
CREATE STREAM recommendation_conflicts AS
SELECT
    patient_id,
    clinical_issue,
    COLLECT_LIST(STRUCT(
        specialty := responding_specialty,
        recommendation := recommendation
    )) as conflicting_recommendations
FROM consultation_responses_stream
WINDOW TUMBLING (SIZE 30 MINUTES)
GROUP BY patient_id, clinical_issue
HAVING COUNT(DISTINCT recommendation) > 1
EMIT CHANGES;

-- Escalate conflicts to senior Digital Person or human
CREATE STREAM escalation_required AS
SELECT
    patient_id,
    clinical_issue,
    conflicting_recommendations,
    'CONFLICT_RESOLUTION' as escalation_type,
    CASE
        WHEN clinical_issue LIKE '%emergency%' THEN 'immediate'
        ELSE 'urgent'
    END as priority
FROM recommendation_conflicts
EMIT CHANGES;
```

---

## 3.4 Audit and Compliance

Every multi-agent interaction must be traceable.

```sql
-- Complete audit trail
CREATE TABLE multi_agent_audit (
    audit_id VARCHAR(50) PRIMARY KEY,
    patient_id VARCHAR(50),
    interaction_type VARCHAR(50),
    participating_dps ARRAY,
    initial_concern TEXT,
    consultations_requested INT,
    consultations_completed INT,
    consensus_reached BOOLEAN,
    final_recommendation TEXT,
    human_override BOOLEAN DEFAULT FALSE,
    total_duration_seconds INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Track learning from consultations
CREATE TABLE consultation_learning (
    learning_id VARCHAR(50) PRIMARY KEY,
    dp_id VARCHAR(50),
    consultation_id VARCHAR(50),
    pattern_identified TEXT,
    knowledge_updated TEXT,
    will_affect_future_decisions BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 3.5 Example: Multi-Agent Coordination Flow

### Scenario: Cardiac Patient with Complex Presentation

```python
# 1. CardioDP detects irregular rhythm + unusual rash
cardiac_event = {
    "patient_id": "P001",
    "irregular_rhythm": True,
    "skin_finding": "purpuric_rash",
    "on_warfarin": True
}

# 2. CardioDP requests consultations
cardio_dp.request_consultation(
    target="dermatology",
    question="Drug reaction vs vasculitis?"
)
cardio_dp.request_consultation(
    target="pharmacy",
    question="Warfarin interaction check"
)

# 3. DermaDP responds
derma_response = {
    "assessment": "Consistent with warfarin-induced skin necrosis",
    "recommendation": "Urgent warfarin cessation",
    "confidence": 0.85
}

# 4. PharmDP responds
pharm_response = {
    "assessment": "INR likely supratherapeutic",
    "recommendation": "Hold warfarin, check INR stat",
    "alternatives": ["heparin bridge", "DOAC consideration"],
    "confidence": 0.90
}

# 5. CardioDP synthesizes
final_plan = cardio_dp.synthesize_consultations([
    derma_response,
    pharm_response
])
# Result: Coordinated plan addressing all concerns
```

---

## 3.6 Advanced Patterns

### Cascading Consultations

One consultation triggers others:

```sql
CREATE STREAM cascading_consultations AS
SELECT
    r1.request_id as initial_request,
    r2.request_id as cascaded_request,
    r1.requesting_specialty as initiator,
    r2.requesting_specialty as intermediate,
    r2.target_specialty as final_target
FROM consultation_requests_stream r1
JOIN consultation_requests_stream r2 WITHIN 1 HOUR
    ON r1.responding_dp_id = r2.requesting_dp_id
WHERE r2.created_at > r1.created_at
EMIT CHANGES;
```

### Specialist Networks

Digital Persons form consultation patterns:

```sql
-- Track which specialists consult each other most
CREATE TABLE consultation_network AS
SELECT
    requesting_specialty,
    target_specialty,
    COUNT(*) as consultation_count,
    AVG(response_time_seconds) as avg_response_time,
    AVG(confidence_score) as avg_confidence
FROM consultation_audit
WINDOW TUMBLING (SIZE 7 DAYS)
GROUP BY requesting_specialty, target_specialty
HAVING COUNT(*) > 10
EMIT CHANGES;
```

---

## 3.7 Implementation Checklist

- [ ] Consultation request/response tables in Snowflake
- [ ] KSQLDB routing streams by specialty
- [ ] Enhanced Digital Person bridges with consultation methods
- [ ] Consensus calculation mechanisms
- [ ] Conflict detection and resolution
- [ ] Audit trail for all interactions
- [ ] Testing with multi-agent scenarios

---

## 3.8 Key Insights

Multi-agent coordination enables:

1. **Specialist collaboration** without human mediation
2. **Rapid consensus** on complex cases
3. **Conflict identification** requiring human review
4. **Knowledge transfer** between Digital Persons
5. **Audit compliance** for all decisions

The architecture mirrors healthcare teams but operates in seconds rather than days. Each Digital Person maintains their expertise while contributing to collective decision-making.

---

## Next Steps

After implementing basic coordination:
1. Add more sophisticated consensus algorithms
2. Implement learning from consultation patterns
3. Create specialist hierarchies (senior vs junior Digital Persons)
4. Add cross-organization consultation capabilities

The foundation in Chapter 3 enables Digital Persons to work as a team, dramatically expanding what autonomous healthcare systems can accomplish.

---

**Implementation Repository:** `implementation/chapter-3/`