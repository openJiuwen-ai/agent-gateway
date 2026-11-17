# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

import json
import threading
from pathlib import Path
from typing import Any

from agent_gateway.common.paths import get_agent_memory_dir, logger

class MessageStore:
    def __init__(self, api_client: Any = None, platform_adapter: Any = None):
        self._memory_dir = (
            get_agent_memory_dir() / "group_chat"
        )  # 群聊记忆目录
        self._memory_file = self._memory_dir / "feishu_memory.json"  # 飞书记忆文件路径（兼容旧逻辑）
        self._memory_lock = threading.Lock()  # 记忆文件读写锁
        self._api_client = api_client  # 飞书API客户端
        self._platform_adapter = platform_adapter  # 平台适配器，用于获取用户信息等

    def set_api_client(self, api_client: Any) -> None:
        """
        设置飞书API客户端。

        Args:
            api_client: 飞书API客户端实例
        """
        self._api_client = api_client
        logger.info("飞书API客户端已设置")
    
    def set_platform_adapter(self, platform_adapter: Any) -> None:
        """
        设置平台适配器。

        Args:
            platform_adapter: 平台适配器实例
        """
        self._platform_adapter = platform_adapter
        logger.info("平台适配器已设置")
    
    def get_user_name_by_open_id(self, open_id: str) -> str:
        """
        获取用户名称，优先使用平台适配器，如果不可用则使用本地API客户端。

        Args:
            open_id: 用户 open_id

        Returns:
            str: 用户名
        """
        # 优先使用平台适配器（如果可用）
        if self._platform_adapter and hasattr(self._platform_adapter, 'get_user_name_by_open_id'):
            return self._platform_adapter.get_user_name_by_open_id(open_id)
        
        # 如果平台适配器不可用，返回空字符串（因为原方法已被移除）
        return ""

    def _get_memory_file_path(self, chat_id: str) -> Path:
        """
        获取指定群聊的记忆文件路径。

        Args:
            chat_id: 群聊ID

        Returns:
            Path: 记忆文件路径
        """
        return self._memory_dir / f"{chat_id}.json"

    def load_memory(self, chat_id: str | None = None) -> dict[str, list] | list:
        """
        加载飞书记忆文件。

        Args:
            chat_id: 群聊ID，如果为None则加载所有记忆（兼容旧逻辑）

        Returns:
            dict或list: 记忆数据
        """
        with self._memory_lock:
            # 如果指定了chat_id，加载该群聊的独立记忆文件
            if chat_id:
                memory_file = self._get_memory_file_path(chat_id)
                logger.info(f"[调试] _load_memory: 群聊记忆文件路径={memory_file}, exists={memory_file.exists()}")
                if not memory_file.exists():
                    logger.info(f"[调试] _load_memory: 群聊记忆文件不存在，返回空列表: chat_id={chat_id}")
                    return []
                try:
                    with open(memory_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        logger.info(f"[调试] _load_memory: 成功加载群聊记忆，消息数={len(data)}, chat_id={chat_id}")
                        return data
                except Exception as e:
                    logger.warning(f"[调试] 加载群聊记忆文件失败: {e}, chat_id={chat_id}")
                    return []
            
            # 兼容旧逻辑：加载统一的feishu_memory.json
            logger.info(f"[调试] _load_memory: 统一记忆文件路径={self._memory_file}, exists={self._memory_file.exists()}")
            if not self._memory_file.exists():
                logger.info(f"[调试] _load_memory: 统一记忆文件不存在，返回空字典")
                return {}
            try:
                with open(self._memory_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    logger.info(f"[调试] _load_memory: 成功加载统一记忆，会话数={len(data)}")
                    return data
            except Exception as e:
                logger.warning(f"[调试] 加载飞书记忆文件失败: {e}")
                return {}

    def _save_memory(self, memory: dict[str, list] | list, chat_id: str | None = None) -> None:
        """
        保存飞书记忆文件。

        Args:
            memory: 记忆数据
            chat_id: 群聊ID，如果为None则保存到统一的feishu_memory.json（兼容旧逻辑）
        """
        with self._memory_lock:
            try:
                self._memory_dir.mkdir(parents=True, exist_ok=True)
                
                # 如果指定了chat_id，保存到该群聊的独立记忆文件
                if chat_id:
                    memory_file = self._get_memory_file_path(chat_id)
                    with open(memory_file, "w", encoding="utf-8") as f:
                        json.dump(memory, f, ensure_ascii=False, indent=2)
                    logger.info(f"[调试] _save_memory: 群聊记忆已保存: {memory_file}, 消息数={len(memory)}")
                else:
                    # 兼容旧逻辑：保存到统一的feishu_memory.json
                    with open(self._memory_file, "w", encoding="utf-8") as f:
                        json.dump(memory, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"保存飞书记忆文件失败: {e}")

    def add_message_to_memory(self, chat_id: str, message: dict) -> None:
        """
        将消息添加到本地记忆。

        Args:
            chat_id: 聊天ID
            message: 消息数据
        """
        # 加载该群聊的记忆
        history = self.load_memory(chat_id)

        open_id = message.get("open_id", "")
        user_name = self.get_user_name_by_open_id(open_id)
        message["user_name"] = user_name
        
        # 添加新消息
        history.append(message)
        
        # 保存到该群聊的独立记忆文件
        self._save_memory(history, chat_id)
        logger.info(f"[调试] 新消息已添加到群聊记忆: chat_id={chat_id}, 总消息数={len(history)}")
