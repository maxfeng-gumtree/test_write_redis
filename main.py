from google.cloud import bigquery
import redis

bq_client = bigquery.Client()

# Redis 客户端初始化
redis_client = redis.Redis(host='relevant-adsearch-redis.europe-west4.internal.gum-algorithm-dev.gumtree.cloud', port=6379, db=0)


query_table="gum-ingestion-prod-ab8b.analytics_420507409.events_20250226"

# BigQuery 查询
query = """
SELECT event_date, event_timestamp, event_name
FROM `gum-ingestion-prod-ab8b.analytics_420507409.events_20250226`
LIMIT 10
"""
query_job = bq_client.query(query)

# 将查询结果写入 Redis
for row in query_job:
    key = f"key:{row['event_date']}"  # 根据需要定义 Redis 的 key
    value = {
        "event_timestamp": row["event_timestamp"],
        "event_name": row["event_name"]
    }
    redis_client.hmset(key, value)  # 使用哈希存储数据

print("Data imported to Redis successfully")