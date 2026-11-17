import os
DATABASE_URL = os.environ['DATABASE_URL']
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
JWT_SECRET = os.environ['JWT_SECRET']
# 执行器租约秒数：心跳超过该时长未续约即视为失联，任务可被自动接管。
RECOVERY_LEASE_SECONDS = int(os.getenv('RECOVERY_LEASE_SECONDS', '30'))
