from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, BooleanType,
    DateType, TimestampType, FloatType, DataType
)

class FakeRowGenerator: 
    def __init__(self, schema, seed=None, locale=None):
        from faker import Faker 
        self.faker = Faker(locale) if locale else Faker()
        if seed:
            self.faker.seed_instance(seed)
        self.schema = schema
        self.generators = self._build_generators()

    def _build_generators(self):
        gen = []
        for field in self.schema.fields:
            if hasattr(self.faker, field.name):
                gen.append(getattr(self.faker, field.name))
            else:
                gen.append(self._fake_for_type(field.dataType))
        return gen
    
    def _fake_for_type(self, datatype: DataType):
        if isinstance(datatype, StringType):
            return self.faker.word
        elif isinstance(datatype, IntegerType):
            return lambda: self.faker.random_int(min=0, max=1000)
        elif isinstance(datatype, FloatType) or isinstance(datatype, DoubleType):
            return lambda: round(self.faker.pyfloat(left_digits=3, right_digits=2), 2)
        elif isinstance(datatype, BooleanType):
            return self.faker.boolean
        elif isinstance(datatype, DateType):
            return self.faker.date
        elif isinstance(datatype, TimestampType):
            return self.faker.date_time
        else:
            return lambda: None

    def generate_rows(self, count):
        for _ in range(count):
            yield tuple(gen() for gen in self.generators)
    