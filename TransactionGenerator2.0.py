# TransactionGenerator.py - Enhanced for fraud like the original ExecuteScript-Fraud.py
from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import datetime
import random
import uuid
import json
import math

# Shared data (same as original)
AMOUNTS = [20, 50, 100, 200, 300, 400, 500]
CITIES = [
    {"lat": 48.8534, "lon": 2.3488, "city": "Paris"},
    {"lat": 43.2961743, "lon": 5.3699525, "city": "Marseille"},
    {"lat": 45.7578137, "lon": 4.8320114, "city": "Lyon"},
    {"lat": 50.6365654, "lon": 3.0635282, "city": "Lille"},
    {"lat": 44.841225, "lon": -0.5800364, "city": "Bordeaux"}
]

class TransactionGenerator(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.2-SNAPSHOT'  # bumped version so NiFi picks up changes
        description = '''A Python processor that creates credit card transactions (normal + occasional fraud) for the Fraud Demo. Mimics the original ExecuteScript-Fraud.py behavior.'''
        tags = ['fraud', 'transaction', 'demo']

    # New: Configurable fraud rate (like the original FRAUD_TICK logic)
    FRAUD_PROBABILITY = PropertyDescriptor(
        name='Fraud Probability',
        description='Probability (0.0 - 1.0) of emitting a fraudulent transaction instead of a normal one. Matches the rarity of frauds in the original script.',
        required=True,
        default_value='0.1',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        expression_language_scope=None  # not needed for this simple value
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.descriptors = [self.FRAUD_PROBABILITY]
        self.last_legit = None  # in-memory state for realistic frauds (same account_id)

    def getPropertyDescriptors(self):
        return self.descriptors

    # Geo helpers (unchanged, now proper instance methods)
    def create_random_point(self, x0, y0, distance):
        r = distance / 111300
        u = random.random()
        v = random.random()
        w = r * math.sqrt(u)
        t = 2 * math.pi * v
        x = w * math.cos(t)
        x1 = x / math.cos(y0)
        y = w * math.sin(t)
        return (x0 + x1, y0 + y)

    def create_geopoint(self, lat, lon):
        return self.create_random_point(lat, lon, 50000)

    def get_latlon(self):
        geo = random.choice(CITIES)
        return self.create_geopoint(geo['lat'], geo['lon']), geo['city']

    def create_fintran(self):
        latlon, city = self.get_latlon()
        tsbis = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date = str(datetime.datetime.strptime(tsbis, "%Y-%m-%d %H:%M:%S"))
        fintran = {
            'ts': date,
            'account_id': str(random.randint(1, 1000)),
            'transaction_id': str(uuid.uuid1()),
            'amount': random.randrange(1, 2000),
            'lat': latlon[0],
            'lon': latlon[1]
        }
        return fintran

    # Fixed & improved: now a proper method, uses self, matches original fraud logic
    def create_fraudtran(self, fintran):
        latlon, city = self.get_latlon()
        # Fraud timestamp is earlier (like original)
        tsbis = (datetime.datetime.now() - datetime.timedelta(seconds=random.randint(60, 600))).strftime("%Y-%m-%d %H:%M:%S")
        fraudtran = {
            'ts': tsbis,
            'account_id': fintran['account_id'],
            'transaction_id': 'xxx' + str(fintran['transaction_id']),
            'amount': random.randrange(1, 2000),
            'lat': latlon[0],
            'lon': latlon[1]
        }
        return fraudtran

    def create(self, context):
        # Read the configurable fraud probability
        fraud_prob = float(context.getProperty(self.FRAUD_PROBABILITY.name).getValue())

        # Decide: fraud or normal?
        if self.last_legit is not None and random.random() < fraud_prob:
            # Emit fraud based on the previous legitimate transaction
            fraudtran = self.create_fraudtran(self.last_legit)
            fintransaction = json.dumps(fraudtran)
            # Optional: you could add an attribute like {'is_fraud': 'true'} if you want downstream routing
            return FlowFileSourceResult(
                relationship='success',
                attributes={'NiFi': 'PythonProcessor', 'fraud': 'true'},
                contents=fintransaction
            )
        else:
            # Emit normal transaction and remember it for future frauds
            fintran = self.create_fintran()
            self.last_legit = fintran
            fintransaction = json.dumps(fintran)
            return FlowFileSourceResult(
                relationship='success',
                attributes={'NiFi': 'PythonProcessor', 'fraud': 'false'},
                contents=fintransaction
            )