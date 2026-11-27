# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""群聊消息改写依赖的 LLM 类型按需加载。

openjiuwen 的 LLM 模块会带出 openai / mcp 等整套模型客户端依赖，而只有群聊
消息改写（``IMConversationProcessor._rewrite_query``）用得到。导入本模块不应
加载它们；首次改写时才加载，且导入不占用事件循环线程。
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

from agent_gateway.im_pipeline import im_inbound
from agent_gateway.im_pipeline.im_inbound import IMConversationProcessor


class _Config(dict):
    """替代 pydantic 配置类：接受任意关键字字段。"""

    def __init__(self, **fields):
        super().__init__(fields)


class _FakeModel:
    def __init__(self, *, model_config, model_client_config):
        self.model_config = model_config
        self.model_client_config = model_client_config

    async def invoke(self, *, model, messages):
        del model
        self.messages = messages
        return SimpleNamespace(content="改写后的消息")


class _Adapter:
    def get_bot_mention_tokens(self) -> list[str]:
        return ["@WorkSwarm"]


def test_import_keeps_llm_sdk_unloaded(tmp_path):
    env = os.environ.copy()
    for key in ('JIUWENSWARM_HOME', 'JIUWENSWARM_DATA_DIR', 'AGENTOS_GATEWAY_LOG_DIR'):
        env[key] = str(tmp_path / key)
    result = subprocess.run(
        [sys.executable, '-c', '''
import importlib, sys
importlib.import_module('agent_gateway.im_pipeline.im_inbound')
for module in ('openai', 'mcp'):
    assert module not in sys.modules, module
'''], env=env, capture_output=True, text=True, timeout=25,
    )
    assert result.returncode == 0, result.stderr


async def test_rewrite_query_loads_llm_types_once_off_the_event_loop(monkeypatch):
    load_threads = []
    types_when_ensuring = []

    def load_types():
        load_threads.append(threading.current_thread())
        return _FakeModel, _Config, _Config

    monkeypatch.setattr(im_inbound, '_load_llm_types', load_types)
    processor = IMConversationProcessor()

    def ensure_llm():
        types_when_ensuring.append(processor._llm_types)
        return _FakeModel(model_config=None, model_client_config=None)

    monkeypatch.setattr(processor, '_ensure_llm', ensure_llm)

    results = [await processor._rewrite_query('@WorkSwarm 帮我看看', '王经理', _Adapter()) for _ in range(2)]

    assert results == ['改写后的消息'] * 2
    assert len(load_threads) == 1
    assert load_threads[0] is not threading.main_thread()
    assert types_when_ensuring == [(_FakeModel, _Config, _Config)] * 2


async def test_rewrite_query_falls_back_when_llm_types_cannot_be_loaded(monkeypatch):
    def load_types():
        raise ImportError('openjiuwen LLM SDK unavailable')

    monkeypatch.setattr(im_inbound, '_load_llm_types', load_types)
    processor = IMConversationProcessor()
    ensure_llm = Mock(side_effect=AssertionError('must not build a model without its types'))
    monkeypatch.setattr(processor, '_ensure_llm', ensure_llm)

    assert await processor._rewrite_query('@WorkSwarm 帮我看看', '王经理', _Adapter()) is None
    ensure_llm.assert_not_called()
    assert processor._llm_types is None


def test_ensure_llm_builds_the_model_from_loaded_types_without_importing(monkeypatch):
    load_types = Mock(side_effect=AssertionError('types were already loaded'))
    monkeypatch.setattr(im_inbound, '_load_llm_types', load_types)
    monkeypatch.setattr(
        im_inbound,
        'build_reasoning_model_request_kwargs',
        lambda **kwargs: {'model_name': kwargs['model_name']},
    )
    processor = IMConversationProcessor()
    processor._llm_types = (_FakeModel, _Config, _Config)

    llm = processor._ensure_llm()

    assert isinstance(llm, _FakeModel)
    assert llm.model_config == {'model_name': processor._model_name}
    assert processor._ensure_llm() is llm
    load_types.assert_not_called()


def test_ensure_llm_loads_types_synchronously_when_not_preloaded(monkeypatch):
    loads = []

    def load_types():
        loads.append(threading.current_thread())
        return _FakeModel, _Config, _Config

    monkeypatch.setattr(im_inbound, '_load_llm_types', load_types)
    monkeypatch.setattr(
        im_inbound,
        'build_reasoning_model_request_kwargs',
        lambda **kwargs: {'model_name': kwargs['model_name']},
    )
    processor = IMConversationProcessor()

    assert isinstance(processor._ensure_llm(), _FakeModel)

    assert loads == [threading.main_thread()]
    assert processor._llm_types == (_FakeModel, _Config, _Config)
