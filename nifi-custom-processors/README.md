# NiFi Custom Python Processors

This directory contains the custom processors built with Apache NiFi 2.0’s official Python API (`nifiapi`).

---

## Processor Development Order & Purpose

The processors were built iteratively, following the exact methodology described in the [AI with NiFi and Python](https://cldr-steven-matison.github.io/blog/How-to-AI-with-NiFi-and-Python/) post:

### 1. `TransactionGenerator.py`
**Type**: `FlowFileSource`  
**Purpose**: First synthetic data generator.  
Creates realistic credit-card transaction FlowFiles (JSON) for testing downstream fraud detection pipelines.  
Supports both “normal” and “fraudulent” transaction modes.

### 2. `NewTransactionGenerator.py`
**Type**: `FlowFileSource`  
**Purpose**: Improved and refactored version of the original transaction generator.  
Contains enhancements and cleaner code developed after initial testing.

### 3. `GenericTransform.py`
**Type**: `FlowFileTransform`  
**Purpose**: **Bare-minimum proven skeleton** (the “framework” described in Rule 2 of the AI guide).  
Used to validate that a custom Python processor loads correctly in the NiFi UI and exposes `success`/`failure` relationships **before** any business logic is added.  
This is the exact template you should start with for any new transform processor.

### 4. `FraudModel.py`
**Type**: `FlowFileTransform`  
**Purpose**: Native implementation of the CML fraud detection model inside NiFi.  
Takes incoming transaction FlowFiles, runs the heuristic rules (high amount + suspicious geography), and enriches the payload with `cml_response` (fraud_score, risk_level, decision, explanations).  
Built on top of the `GenericTransform` skeleton using the defensive patterns from the AI guide (list vs. dict handling, try/except, never overwriting original payload, etc.).

---

## Future Processors

As more processors are added they will be appended here in development order with a short purpose description.