from typing import Dict
from decimal import Decimal
from datetime import datetime


class Trip:
    def __init__(self, d: Dict):
        self.lpep_pickup_datetime = d['lpep_pickup_datetime']
        self.lpep_dropoff_datetime = d['lpep_dropoff_datetime']
        self.PULocationID = d['PULocationID']
        self.DOLocationID = d['DOLocationID']
        self.passenger_count = '0' if d['passenger_count'] == '' else d['passenger_count']
        self.trip_distance = d['trip_distance']
        self.tip_amount = d['tip_amount']
    
    def __repr__(self):
        return f'{self.__dict__}'
    
    def to_dict(self) -> Dict:
        return {
            'lpep_pickup_datetime': self.lpep_pickup_datetime,
            'lpep_dropoff_datetime': self.lpep_dropoff_datetime,
            'PULocationID': self.PULocationID,
            'DOLocationID': self.DOLocationID,
            'passenger_count': self.passenger_count,
            'trip_distance': self.trip_distance,
            'tip_amount': self.tip_amount
        }