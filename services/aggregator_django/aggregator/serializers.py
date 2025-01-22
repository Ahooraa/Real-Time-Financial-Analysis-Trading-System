from rest_framework import serializers
# Define valid fields to prevent SQL injection
VALID_FIELDS = {
    'stock_symbol', 'signal', 'local_time', 'open', 'close',
    'high', 'low', 'volume', 'SMA_5', 'EMA_10', 'delta', 'gain',
    'loss', 'avg_gain_10', 'avg_loss_10', 'rs', 'RSI_10'
}

AGGREGATION_CHOICES = (
    ('avg', 'Average'),
    ('highest', 'Highest'),
    ('lowest', 'Lowest'),
)

class AggregationRequestSerializer(serializers.Serializer):
    aggregation = serializers.ChoiceField(choices=AGGREGATION_CHOICES)
    stock_symbol = serializers.CharField(max_length=50)
    period = serializers.IntegerField(min_value=1)  # Period in minutes
    field = serializers.ChoiceField(choices=VALID_FIELDS)
