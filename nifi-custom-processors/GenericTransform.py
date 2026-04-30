import json
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class GenericTransformTemplate(FlowFileTransform):
    # This Java inner class is mandatory. It registers the processor in the NiFi backend.
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'A proven, annotated template for a NiFi Python FlowFileTransform processor.'
        tags = ['template', 'python', 'transform']

    def __init__(self, **kwargs):
        # Using 'pass' is the safest initialization for this specific NiFi environment bridge
        pass

    def transform(self, context, flowfile):
        # STEP 1: Read the incoming FlowFile payload into a string
        contents_str = flowfile.getContentsAsBytes().decode('utf-8')
        
        # STEP 2: Extract existing attributes so they aren't lost in transit
        attributes = flowfile.getAttributes()
        
        try:
            # STEP 3: Execute your custom logic (e.g., JSON parsing/manipulation)
            # data = json.loads(contents_str)
            # data['processed_by'] = 'GenericTemplate'
            # output_str = json.dumps(data)
            output_str = contents_str # Pass-through for this example
            
            # STEP 4: Add any new NiFi attributes for downstream routing
            attributes['python_processor_status'] = 'success'
            
            # STEP 5: Route to 'success' with the preserved attributes and modified content
            return FlowFileTransformResult(
                relationship='success', 
                attributes=attributes, 
                contents=output_str
            )
            
        except Exception as e:
            # STEP 6: If logic fails, route to 'failure' safely instead of crashing the processor
            attributes['python_error'] = str(e)
            return FlowFileTransformResult(
                relationship='failure', 
                attributes=attributes, 
                contents=contents_str
            )