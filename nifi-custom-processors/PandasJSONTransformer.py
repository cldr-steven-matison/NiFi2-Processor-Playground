import json
import io
import pandas as pd
import numpy as np
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class PandasJSONTransformer(FlowFileTransform):
    class Java:
        # Essential: Ensures success and failure relationships appear in NiFi
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.7-FINAL'
        description = 'An example processor using python pandas.'
        tags = ['pandas', 'poc', 'geospatial']
        dependencies = ['pandas', 'numpy'] # NiFi auto-installs these

    def __init__(self, **kwargs):
        # 'pass' is the safest initialization for this environment
        pass

    def transform(self, context, flowfile):
        content_bytes = flowfile.getContentsAsBytes()
        attributes = flowfile.getAttributes()

        # Merritt Island, FL Coordinates
        HOME_LAT, HOME_LON = 28.3181, -80.6660

        try:
            # Step 1: Handle the "Array Trap"
            # Even for single records, we wrap in a list so Pandas creates a proper DataFrame row
            raw_data = json.loads(content_bytes.decode('utf-8'))
            if not isinstance(raw_data, list):
                raw_data = [raw_data]

            df = pd.DataFrame(raw_data)

            # Step 2: Proof of Concept Math
            if 'lat' in df.columns and 'lon' in df.columns:
                df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
                df['lon'] = pd.to_numeric(df['lon'], errors='coerce')

                # Calculate Euclidean distance from Merritt Island:
                # dist = sqrt((lat1 - lat2)^2 + (lon1 - lon2)^2)
                df['dist_from_home'] = np.sqrt(
                    (df['lat'] - HOME_LAT)**2 + (df['lon'] - HOME_LON)**2
                )
                
                # Add a simple flag to show Pandas touched the data
                df['pandas_processed'] = True

            # Step 3: Output Generation
            output_json = df.to_json(orient='records', indent=None)
            
            return FlowFileTransformResult(
                relationship='success',
                contents=output_json.encode('utf-8'),
                attributes={
                    **attributes,
                    'pandas.transformed': 'true',
                    'pandas.version': pd.__version__
                }
            )

        except Exception as e:
            # Rule 3: Defensive failure routing
            return FlowFileTransformResult(
                relationship='failure',
                contents=content_bytes,
                attributes={**attributes, 'pandas.error': str(e)}
            )