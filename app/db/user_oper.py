from typing import Optional

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from app import schemas
from app.core.security import verify_token
from app.db import DbOper, get_db
from app.db.models.user import User


def get_current_user(
        db: Session = Depends(get_db),
        token_data: schemas.TokenPayload = Depends(verify_token)
) -> User:
    """
    获取当前用户
    """
    user = User.get(db, rid=token_data.sub)
    if not user:
        raise HTTPException(status_code=403, detail="用户不存在")
    return user


def get_current_active_user(
        current_user: User = Depends(get_current_user),
) -> User:
    """
    获取当前激活用户
    """
    if not current_user.is_active:
        raise HTTPException(status_code=403, detail="用户未激活")
    return current_user


def get_current_active_superuser(
        current_user: User = Depends(get_current_user),
) -> User:
    """
    获取当前激活超级管理员
    """
    if not current_user.is_superuser:
        raise HTTPException(
            status_code=400, detail="用户权限不足"
        )
    return current_user


class UserOper(DbOper):
    """
    用户管理
    """

    def add(self, **kwargs):
        """
        新增用户
        """
        user = User(**kwargs)
        user.create(self._db)

    def get_by_name(self, name: str) -> User:
        """
        根据用户名获取用户
        """
        return User.get_by_name(self._db, name)

    def get_permissions(self, name: str) -> dict:
        """
        获取用户权限
        {
            "admin": "管理员",
            "usermanage": "用户管理",
            "dashboard": "仪表板",
            "ranking": "推荐榜单",
            "resource": {
                "search": "搜索站点资源",
                "download": "下载站点资源",
            },
            "subscribe": {
                "request": "提交订阅请求",
                "autopass": "订阅请求自动批准"
                "approve": "审批订阅请求",
                "calendar": "查看订阅日历",
                "manage": "管理所有订阅"
            },
            "downloading": {
                "view": "查看正在下载任务",
                "manager": "管理正在下载任务"
            }
        }
        """
        user = User.get_by_name(self._db, name)
        if user:
            return user.permissions or {}
        return {}

    def get_settings(self, name: str) -> Optional[dict]:
        """
        获取用户个性化设置，返回None表示用户不存在
        """
        user = User.get_by_name(self._db, name)
        if user:
            return user.settings or {}
        return None

    def get_setting(self, name: str, key: str) -> Optional[str]:
        """
        获取用户个性化设置
        """
        settings = self.get_settings(name)
        if settings:
            return settings.get(key)
        return None
