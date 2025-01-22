# services/aggregator_django/aggregator/views.py

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from .serializers import AggregationRequestSerializer
import pytz

def get_questdb_connection():
    """
    Establishes a connection to QuestDB.
    """
    return psycopg2.connect(
        dbname=settings.DATABASES['questdb']['NAME'],
        user=settings.DATABASES['questdb']['USER'],
        password=settings.DATABASES['questdb']['PASSWORD'],
        host=settings.DATABASES['questdb']['HOST'],
        port=settings.DATABASES['questdb']['PORT']
    )

class AggregationView(APIView):
    """
    API View to handle aggregation requests: avg, highest, lowest.
    """

    def post(self, request, format=None):
        serializer = AggregationRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        aggregation = serializer.validated_data['aggregation']
        stock_symbol = serializer.validated_data['stock_symbol']
        period = serializer.validated_data['period']
        field = serializer.validated_data['field']

        # Define the timezone as Asia/Tehran
        tehran_tz = pytz.timezone('Asia/Tehran')

        # Get the current time in Asia/Tehran timezone
        end_time = datetime.now(tehran_tz)

        # Calculate the start time by subtracting the period
        start_time = end_time - timedelta(minutes=period)

        # Convert datetime objects to naive datetime (remove timezone info)
        # This is crucial to prevent QuestDB from interpreting them as timestamptz
        start_time_naive = start_time.replace(tzinfo=None)
        end_time_naive = end_time.replace(tzinfo=None)

        # Format the naive datetime objects as ISO 8601 strings without timezone
        # Example format: '2025-01-23T02:33:22'
        start_time_str = start_time_naive.isoformat(sep=' ')
        end_time_str = end_time_naive.isoformat(sep=' ')

        try:
            conn = get_questdb_connection()
            cursor = conn.cursor()

            # Dynamically construct the SQL query based on the aggregation type
            if aggregation == 'avg':
                query = sql.SQL("""
                    SELECT AVG({field})
                    FROM stock_data
                    WHERE stock_symbol = %s
                      AND local_time >= %s
                      AND local_time <= %s
                """).format(field=sql.Identifier(field))
                cursor.execute(query, [stock_symbol, start_time_str, end_time_str])
                result = cursor.fetchone()
                aggregation_result = result[0]
                response_data = {'avg': aggregation_result}

            elif aggregation == 'highest':
                query = sql.SQL("""
                    SELECT MAX({field})
                    FROM stock_data
                    WHERE stock_symbol = %s
                      AND local_time >= %s
                      AND local_time <= %s
                """).format(field=sql.Identifier(field))
                cursor.execute(query, [stock_symbol, start_time_str, end_time_str])
                result = cursor.fetchone()
                aggregation_result = result[0]
                response_data = {'highest': aggregation_result}

            elif aggregation == 'lowest':
                query = sql.SQL("""
                    SELECT MIN({field})
                    FROM stock_data
                    WHERE stock_symbol = %s
                      AND local_time >= %s
                      AND local_time <= %s
                """).format(field=sql.Identifier(field))
                cursor.execute(query, [stock_symbol, start_time_str, end_time_str])
                result = cursor.fetchone()
                aggregation_result = result[0]
                response_data = {'lowest': aggregation_result}

            else:
                return Response({'error': 'Invalid aggregation type.'}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({'error': 'Database error', 'details': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return Response(response_data, status=status.HTTP_200_OK)
