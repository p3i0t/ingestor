import pyarrow as pa
from ingestor.exceptions import IngestorInvalidArgumentError


def generate_snapshot_schema(
    level: int = 5, 
    include_amount: bool = False,
    timestamp_as_int64: bool = True,
    is_stock: bool = False
) -> pa.Schema:
    """generate pyarrow schema for snapshot data.

    Args:
        level (int, optional): number of snapshot levels. Defaults to 5.
        include_amount (bool, optional): _description_. Defaults to False.
        timestamp_as_int64 (bool, optional): indicator to use int64 for timestamp. Defaults to True.
        is_stock (bool, optional): indicator to use stock schema. Defaults to False.

    Raises:
        IngestorInvalidArgumentError: timestamp has to be int64 for now.

    Returns:
        pa.Schema: pyarrow schema for snapshot data.
    """
    if not timestamp_as_int64:
        raise IngestorInvalidArgumentError(
            'Only int64 timestamp is supported for deltalake.')
    fields = [
        pa.field('id', pa.int64(), nullable=False),  # tick id
        pa.field('symbol', pa.string(), nullable=False),  # categorical: pa.field('symbol', pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field('datetime', pa.int64(), nullable=False),  # datetime eee: pa.timestamp('ns')
        pa.field('last_price', pa.float64()),  # last price
        pa.field('highest_price', pa.float64()),  # highest price
        pa.field('lowest_price', pa.float64()),  # lowest price
        pa.field('average', pa.float64()),  # average price
        pa.field('volume', pa.float64()),  # volume
        pa.field('amount', pa.float64()),  # amount
        
    ]
    if not is_stock:
        fields.append(
            pa.field('open_interest', pa.float64())) # open interest
    for i in range(1, level+1):
        fields.append(pa.field(f'bid_price{i}', pa.float64()))
        fields.append(pa.field(f'bid_volume{i}', pa.float64()))
        if include_amount:
            fields.append(pa.field(f'bid_amount{i}', pa.float64()))
            
        fields.append(pa.field(f'ask_price{i}', pa.float64()))
        fields.append(pa.field(f'ask_volume{i}', pa.float64()))
        if include_amount:
            fields.append(pa.field(f'ask_amount{i}', pa.float64()))
    
    return pa.schema(fields)
