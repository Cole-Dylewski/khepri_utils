from django.db import models
import datetime as dt
from decimal import Decimal
import uuid
import ipaddress
import pandas as pd

from khepri_utils.common import basic

django_model_field_desc = {
    'AutoField': 'An integer field that automatically increments.',
    'BigAutoField': 'A 64-bit integer field that automatically increments.',
    'BigIntegerField': 'A 64-bit integer field.',
    'BinaryField': 'A field to store binary data.',
    'BooleanField': 'A boolean (True/False) field.',
    'CharField': 'A field to store a fixed-length string.',
    'DateField': 'A field to store a date (year, month, day).',
    'DateTimeField': 'A field to store a date and time.',
    'DecimalField': 'A fixed-precision decimal field.',
    'DurationField': 'A field to store a duration of time.',
    'EmailField': 'A field to store an email address.',
    'FileField': 'A field to upload files.',
    'FloatField': 'A floating-point number field.',
    'ImageField': 'A field to upload image files.',
    'IntegerField': 'An integer field.',
    'GenericIPAddressField': 'A field to store an IPv4 or IPv6 address.',
    'PositiveIntegerField': 'An integer field that only allows positive values.',
    'PositiveSmallIntegerField': 'A smaller integer field that only allows positive values.',
    'SlugField': 'A field to store URL-friendly strings.',
    'SmallIntegerField': 'A smaller integer field.',
    'TextField': 'A field to store long text.',
    'TimeField': 'A field to store time.',
    'URLField': 'A field to store a URL.',
    'ForeignKey': 'A field to define a many-to-one relationship.',
    'ManyToManyField': 'A field to define a many-to-many relationship.',
    'OneToOneField': 'A field to define a one-to-one relationship.',
    'UUIDField': 'A field to store a universally unique identifier (UUID).',
    'HStoreField': 'A field to store key-value pairs in a dictionary-like format (requires PostgreSQL).',
    'JSONField': 'A field to store JSON data (requires PostgreSQL or MariaDB 10.2+).',
    'XMLField': 'A field to store XML data (requires PostgreSQL).',
    'ArrayField': 'A field to store arrays of data (requires PostgreSQL).'
}

django_model_field_types = {
    'AutoField': int,
    'BigAutoField': int,
    'BigIntegerField': int,
    'BinaryField': bytes,
    'BooleanField': bool,
    'CharField': str,
    'DateField': dt.date,
    'DateTimeField': dt.datetime,
    'DecimalField': Decimal,
    'DurationField': dt.timedelta,
    'EmailField': str,
    'FileField': None,  # Depends on the file type
    'FloatField': float,
    'ImageField': None,  # Depends on the image type
    'IntegerField': int,
    'GenericIPAddressField': str,
    'PositiveIntegerField': int,
    'PositiveSmallIntegerField': int,
    'SlugField': str,
    'SmallIntegerField': int,
    'TextField': str,
    'TimeField': dt.time,
    'URLField': str,
    'ForeignKey': None,  # Depends on the related model
    'ManyToManyField': None,  # Depends on the related model
    'OneToOneField': None,  # Depends on the related model
    'UUIDField': uuid.UUID,
    'HStoreField': dict,
    'JSONField': dict,
    'XMLField': str,
    'ArrayField': list,
}
                
python_to_django_model_field_types = {
    str: 'models.CharField(max_length=255)',
    type(None):  'models.CharField(max_length=255)',
    bool: 'models.BooleanField()',
    bytes: 'models.BinaryField()',
    dt.date: 'models.DateField()',
    dt.datetime: 'models.DateTimeField()',
    dt.time: 'models.TimeField()',
    list: 'models.TextField()',
    dict: 'models.JSONField()',
    uuid.UUID: 'models.UUIDField()',
    int: 'models.IntegerField()',
    float: 'models.FloatField()',
    ipaddress.IPv4Address: "models.GenericIPAddressField(protocol='IPv4')",
    ipaddress.IPv6Address: "models.GenericIPAddressField(protocol='IPv6')",
}

def value_to_model_field_type(value):
    for python_type, model_field_type in python_to_django_model_field_types.items():
        # print(value, type(value))
        value = basic.get_best_data_type(value)
        # print(value, type(value))
        if isinstance(value, python_type):
            return model_field_type

def dict_to_model_field_type(data):
    outputData = {}
    for key, value in data.items():
        # print(key, value, type(value))
        legacyType = type(value)
        # print(key)
        model_field_type = value_to_model_field_type(value)
        # print('MODEL FIELD TYPE', model_field_type)
        # print(key,value,legacyType, model_field_type )
        if model_field_type:
            outputData[key] = model_field_type
    return outputData
#takes a dictionary and prints the code to convert it to a django model class
def dict_to_model_class(table_name, data):
    #convert every key to lower case
       
    data = format_model_dict(table_name, data)
    
    output_str = f'''class {table_name}(models.Model): \n'''
    output_str += '\t'+'id = models.AutoField(primary_key=True)\n'
    # output_str += '\t'+'uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)\n'

    # basic.print_nested_obj(newData)  
    output_data = dict_to_model_field_type(data) 
    for k,v in output_data.items():
        output_str += '\t'+k+' = ' + v + '\n'
    
    return output_str


def format_model_dict(table_name, data):
    data = {k.lower().replace('.','_'):v for k,v in data.items()}
    newData = {}
    #check if protected fields exist
    is_id_key = 'id' in data.keys()
    is_table_key = f'{table_name.lower()}_id' in data.keys()
    
    if is_id_key and not is_table_key:
        newData[f'{table_name.lower()}_id'] = data['id']
        del data['id']
        
    for k,v in data.items():
        for k,v in data.items():
            if v is None or pd.isna(v) or pd.isnull(v):
                newData[k] = ''
            else:
                newData[k] = v 
    return newData