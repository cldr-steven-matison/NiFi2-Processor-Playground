import json
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class FraudModel(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.4-SNAPSHOT'
        description = 'Executes the CML fraud detection model natively in NiFi.'
        tags = ['fraud', 'detection', 'cml', 'replacement']

    def __init__(self, **kwargs):
        pass

    # ==========================================
    # CML MODEL LOGIC
    # ==========================================
    SUSPICIOUS_CITIES = {
        "Lagos": {"lat": 6.5244, "lon": 3.3792},
        "New Delhi": {"lat": 28.6139, "lon": 77.2090}
    }
    TOLERANCE = 0.5
    DEMO_SAFE_ACCOUNTS = []

    def is_suspicious_location(self, lat: float, lon: float) -> str:
        for city, coords in self.SUSPICIOUS_CITIES.items():
            if (abs(lat - coords["lat"]) <= self.TOLERANCE) and (abs(lon - coords["lon"]) <= self.TOLERANCE):
                return city
        return None

    def detect_fraud(self, args: dict) -> dict:
        is_fraud = False
        explanations = {}
        
        # Rule 1: High Amount Threshold
        if args.get("amount", 0) > 10000:
            is_fraud = True
            explanations["amount"] = f"Transaction amount ({args.get('amount')}) exceeds the 10,000 limit."

        # Rule 2: Originates strictly around restricted geographies
        if args.get("account_id") not in self.DEMO_SAFE_ACCOUNTS:
            suspicious_city = self.is_suspicious_location(args.get("lat", 0.0), args.get("lon", 0.0))
            if suspicious_city:
                is_fraud = True
                explanations["location"] = f"Transaction originated from a high-risk region near {suspicious_city}."

        if is_fraud:
            return {
                "fraud_score": 0.99,
                "risk_level": "HIGH",
                "decision": "REVIEW",
                "explanations": explanations
            }
        else:
            return {
                "fraud_score": 0.01,
                "risk_level": "LOW",
                "decision": "APPROVE",
                "explanations": {"status": "all heuristic checks passed"}
            }
    # ==========================================

    def transform(self, context, flowfile):
        contents_str = flowfile.getContentsAsBytes().decode('utf-8')
        attributes = flowfile.getAttributes()
        
        try:
            # Parse incoming JSON
            payload = json.loads(contents_str)
            
            # The upstream generator sometimes creates lists of transactions.
            # Handle both lists and single dictionaries safely.
            if isinstance(payload, list):
                for tx in payload:
                    tx["cml_response"] = self.detect_fraud(tx)
                enriched_data = payload
            else:
                payload["cml_response"] = self.detect_fraud(payload)
                enriched_data = payload

            return FlowFileTransformResult(
                relationship='success', 
                attributes=attributes, 
                contents=json.dumps(enriched_data)
            )
            
        except Exception as e:
            # If JSON parsing fails, route to failure and tag the error
            attributes['cml_error'] = str(e)
            return FlowFileTransformResult(
                relationship='failure', 
                attributes=attributes, 
                contents=contents_str
            )