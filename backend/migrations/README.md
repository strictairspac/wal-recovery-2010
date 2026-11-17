# 初始化结构
SQLAlchemy metadata.create_all首次建users、log_sets、crash_cases、recoveries、audits。log_sets.name、crash_cases.name唯一，关联使用外键，入队冻结日志和崩溃点快照。暂无在线schema升级器。

队列生命周期升级：recoveries新增attempt（围栏令牌）、owner、claimed_at、heartbeat_at、lease_expires_at、claim_reason、cancel_reason、finished_at；新增recovery_events记录入队/领取/接管/取消/完成/失败/结果丢弃。src/config/schema.py在种子启动时幂等执行ALTER TABLE ADD COLUMN与索引创建，无需在线升级器；既有记录attempt默认为0，可继续查看、取消和导出。
