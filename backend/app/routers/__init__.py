"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import predict_batch_entries, predict_delivery, resource

# 分片交付的固定路径必须先于通用资源路由注册，否则会被 /{module}/{entry_id} 抢先匹配。
ROUTERS = [predict_delivery, predict_batch_entries, resource]
