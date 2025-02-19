import clickhouse_connect
from clickhouse_connect.driver import httputil

def get_clickhouse_client():
    insight_host = '10.10.9.31'
    username = 'dashboard_user'
    password = 'dA5Hb0#6duS36'
    port = 8123
    
    big_pool_mgr = httputil.get_pool_manager(maxsize=30, num_pools=20)
    
    return clickhouse_connect.get_client(
        host=insight_host,
        username=username,
        password=password,
        port=port,
        query_limit=10000000000,
        connect_timeout='100000',
        send_receive_timeout='300000',
        settings={
            'max_insert_threads': 32,
            'max_query_size': 1000000000,
            'receive_timeout': 0,
            'max_memory_usage': 101737418240
        },
        pool_mgr=big_pool_mgr
    )