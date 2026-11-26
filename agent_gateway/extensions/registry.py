from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable


from agent_gateway.agent_client_mode import AgentClientModeError
from agent_gateway.extensions.callback_compat import unregister_callback_sync
from agent_gateway.extensions.sdk.crypto_utility import CryptoUtility
from agent_gateway.extensions.types import ExtensionConfig
from agent_gateway.extensions import base_crypto
from agent_gateway.extensions.base_crypto import CryptoProvider

if TYPE_CHECKING:
    from openjiuwen.core.runner.callback.framework import AsyncCallbackFramework
    from agent_gateway.common.client.agent_client import AgentServerClient
    from agent_gateway.common.client.third_agent import ThirdAgent
    from agent_gateway.extensions.sdk.agent_server_client import (
        AgentServerClientExtension,
    )
    from agent_gateway.extensions.sdk.application_plugin import (
        ApplicationPluginExtension,
    )
    from agent_gateway.extensions.sdk.connector_source import ConnectorSourceExtension
    from agent_gateway.extensions.sdk.skill_source import SkillSourceExtension
    from agent_gateway.extensions.sdk.third_agent import ThirdAgentExtension
else:
    # Keep runtime type-hint introspection valid without importing Gateway and
    # transport adapters into a Runtime-direct process.
    AgentServerClientExtension = Any
    ApplicationPluginExtension = Any
    ThirdAgentExtension = Any
    AgentServerClient = Any
    ThirdAgent = Any
    # Skill / Connector Source SPI 与上方扩展基类同属 gateway，运行期仍不导入扩展框架。
    SkillSourceExtension = Any
    ConnectorSourceExtension = Any
    AsyncCallbackFramework = Any


class _RegistryCryptoBridge:
    """把 ExtensionRegistry 的 crypto 扩展桥接到 ``common.security.base_crypto`` 全局钩子。

    common（如 config 的 api_key 解密）不能 import 扩展框架（gateway 拆分约束），
    改经 base_crypto 钩子获取 provider。按调用时点解析 registry 实例与 crypto
    扩展（未就绪时原样返回/由调用方回退原文），与原 ``sys.modules`` 软查找的
    惰性语义一致。
    """
    @staticmethod
    def encrypt(plaintext: str, **kwargs) -> str:
        ext = ExtensionRegistry.get_instance().get_crypto_utility_extension()
        provider = ext.get_crypto() if ext is not None else None
        return provider.encrypt(plaintext, **kwargs) if provider is not None else plaintext

    @staticmethod
    def decrypt(ciphertext: str, **kwargs) -> str:
        ext = ExtensionRegistry.get_instance().get_crypto_utility_extension()
        provider = ext.get_crypto() if ext is not None else None
        return provider.decrypt(ciphertext, **kwargs) if provider is not None else ciphertext


class _ApplicationPluginChannel:
    def __init__(self, channel: Any, plugin: ApplicationPluginExtension):
        self._channel = channel
        self._plugin = plugin

    def __getattr__(self, name: str) -> Any:
        return getattr(self._channel, name)

    def register_method(
        self,
        method: str,
        handler: Callable,
        *,
        local_only: bool = False,
        available_when_disabled: bool = False,
    ) -> None:
        async def enabled_handler(ws, req_id, params, session_id):  # noqa: ANN001
            if not available_when_disabled and not self._plugin.is_enabled():
                await self._channel.send_response(
                    ws,
                    req_id,
                    ok=False,
                    error=f"application plugin {self._plugin.plugin_id} is disabled",
                    code="APPLICATION_PLUGIN_DISABLED",
                )
                return
            await handler(ws, req_id, params, session_id)

        self._channel.register_method(method, enabled_handler, local_only=local_only)


class ExtensionRegistry:
    _instance: "ExtensionRegistry | None" = None

    def __init__(
        self,
        callback_framework: AsyncCallbackFramework | None,
        config: dict[str, Any],
        logger: Any,
        callback_framework_factory: Callable[[], AsyncCallbackFramework] | None = None,
        callback_module_preloader: Callable[[], None] | None = None,
    ):
        self._agent_server_client: AgentServerClientExtension | None = None
        self._crypto_tool: CryptoUtility | None = None
        self._third_agent: ThirdAgentExtension | None = None
        self._application_plugins: dict[str, ApplicationPluginExtension] = {}
        # provider_type → SkillSourceExtension factory (Skill Source SPI v2)
        self._skill_source_extensions: dict[str, SkillSourceExtension] = {}
        # provider_type → ConnectorSourceExtension factory (Connector Source SPI)
        self._connector_source_extensions: dict[str, ConnectorSourceExtension] = {}
        if callback_framework is None and callback_framework_factory is None:
            raise ValueError("A callback framework or factory is required")
        self._callback_framework = callback_framework
        self._callback_candidate = None
        self._callback_factory = callback_framework_factory
        self._callback_preloader = callback_module_preloader
        self._callback_import_task: asyncio.Task | None = None
        self._pending_callbacks: list[tuple[str, str, Callable | None, int, dict]] = []
        self._closed = False
        self._config = ExtensionConfig(config=config, logger=logger)

    @property
    def has_deferred_callbacks(self) -> bool:
        return self._callback_framework is None and bool(self._pending_callbacks)

    @property
    def callback_framework(self) -> AsyncCallbackFramework:
        """Retain synchronous access for legacy extensions that require the SDK."""
        if self._closed:
            raise RuntimeError("Extension registry is closed")
        if self._callback_framework is None:
            if self._callback_candidate is None:
                self._callback_candidate = self._callback_factory()
            framework = self._callback_candidate
            # Replay on the owner loop, before publishing the ready framework.
            while self._pending_callbacks:
                operation, event, handler, priority, kwargs = self._pending_callbacks[0]
                if operation == "register":
                    framework.register_sync(event, handler, priority=priority, **kwargs)
                else:
                    unregister_callback_sync(framework, event, handler)
                self._pending_callbacks.pop(0)
            self._callback_framework = framework
            self._callback_candidate = None
        return self._callback_framework

    @callback_framework.setter
    def callback_framework(self, framework: AsyncCallbackFramework) -> None:
        if self._pending_callbacks:
            raise RuntimeError("Cannot replace a callback framework with pending registrations")
        self._callback_framework = framework

    async def prepare_callback_framework(self) -> AsyncCallbackFramework:
        if self._closed:
            raise RuntimeError("Extension registry is closed")
        if self._callback_framework is None and self._callback_preloader is not None:
            task = self._callback_import_task
            if task is None:
                task = asyncio.create_task(
                    asyncio.to_thread(self._callback_preloader), name="gateway-sdk-import",
                )
                self._callback_import_task = task
            # One cancelled caller must not cancel another caller's SDK preparation.
            try:
                await asyncio.shield(task)
            except Exception:
                # A temporary import failure must not poison all later hooks.
                # Another waiter may already have started the replacement task.
                if self._callback_import_task is task:
                    self._callback_import_task = None
                raise
        return self.callback_framework

    async def aclose(self) -> None:
        self._closed = True
        if self._callback_import_task is not None:
            # Synchronous Python imports cannot be forcibly cancelled safely.
            await asyncio.gather(self._callback_import_task, return_exceptions=True)
        self._pending_callbacks.clear()

    @classmethod
    def get_instance(cls) -> "ExtensionRegistry":
        if cls._instance is None:
            raise RuntimeError("ExtensionRegistry 尚未初始化，请先调用 create_instance()")
        return cls._instance

    @classmethod
    def create_instance(
        cls,
        callback_framework: AsyncCallbackFramework | None,
        config: dict[str, Any],
        logger: Any,
        callback_framework_factory: Callable[[], AsyncCallbackFramework] | None = None,
        callback_module_preloader: Callable[[], None] | None = None,
    ) -> "ExtensionRegistry":
        if cls._instance is not None:
            raise RuntimeError("ExtensionRegistry 已初始化，请勿重复调用 create_instance()")
        cls._instance = cls(
            callback_framework=callback_framework,
            config=config,
            logger=logger,
            callback_framework_factory=callback_framework_factory,
            callback_module_preloader=callback_module_preloader,
        )
        # 桥接 crypto 扩展到 common 全局钩子（common/config 的 api_key 解密入口）。
        base_crypto.set_crypto_provider(_RegistryCryptoBridge())
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None
        # registry 实例重置后，common 侧不得再持有旧实例的 crypto 桥接
        base_crypto.set_crypto_provider(None)

    def register_agent_server_client(self, extension: AgentServerClientExtension) -> None:
        if self._agent_server_client is extension:
            # 同一实例重复注册是幂等的；不同客户端扩展才是冲突。
            return
        if self._agent_server_client is not None:
            def label(item: Any) -> str:
                metadata = getattr(item, "metadata", None)
                return str(getattr(metadata, "id", None) or type(item).__name__)

            raise AgentClientModeError(
                "AgentServerClient already registered: "
                f"{label(self._agent_server_client)}; cannot register {label(extension)}. "
                "Select exactly one southbound client extension."
            )
        self._agent_server_client = extension

    def register_crypto_utility(self, extension: CryptoUtility) -> None:
        self._crypto_tool = extension

    def register_third_agent(self, extension: ThirdAgentExtension) -> None:
        self._third_agent = extension

    def register_application_plugin(
        self,
        extension: ApplicationPluginExtension,
    ) -> None:
        plugin_id = str(extension.plugin_id or "").strip()
        if not plugin_id:
            plugin_id = str(extension.metadata.id or "").strip()
        if not plugin_id:
            raise ValueError("application plugin id must not be empty")
        if plugin_id in self._application_plugins:
            raise ValueError(f"application plugin already registered: {plugin_id}")
        self._application_plugins[plugin_id] = extension

    def register_skill_source_extension(
        self,
        extension: SkillSourceExtension,
    ) -> None:
        """Register a Skill Source factory keyed by ``provider_type``."""
        provider_type = str(getattr(extension, "provider_type", "") or "").strip()
        if not provider_type:
            raise ValueError("skill source provider_type must not be empty")
        if provider_type in self._skill_source_extensions:
            raise ValueError(
                f"skill source provider_type already registered: {provider_type}"
            )
        self._skill_source_extensions[provider_type] = extension

    def list_skill_source_extensions(self) -> list[SkillSourceExtension]:
        """Return registered Skill Source factories (order is insertion order)."""
        return list(self._skill_source_extensions.values())

    def get_skill_source_extension(
        self,
        provider_type: str,
    ) -> SkillSourceExtension | None:
        key = str(provider_type or "").strip()
        if not key:
            return None
        return self._skill_source_extensions.get(key)

    def register_connector_source_extension(
        self,
        extension: ConnectorSourceExtension,
    ) -> None:
        """Register a Connector Source factory keyed by ``provider_type``."""
        provider_type = str(getattr(extension, "provider_type", "") or "").strip()
        if not provider_type:
            raise ValueError("connector source provider_type must not be empty")
        if provider_type in self._connector_source_extensions:
            raise ValueError(
                f"connector source provider_type already registered: {provider_type}"
            )
        self._connector_source_extensions[provider_type] = extension

    def list_connector_source_extensions(self) -> list[ConnectorSourceExtension]:
        """Return registered Connector Source factories (order is insertion order)."""
        return list(self._connector_source_extensions.values())

    def get_connector_source_extension(
        self,
        provider_type: str,
    ) -> ConnectorSourceExtension | None:
        key = str(provider_type or "").strip()
        if not key:
            return None
        return self._connector_source_extensions.get(key)

    def get_application_plugins(self) -> tuple[ApplicationPluginExtension, ...]:
        return tuple(self._application_plugins.values())

    def get_application_plugin(
        self,
        plugin_id: str,
    ) -> ApplicationPluginExtension | None:
        return self._application_plugins.get(plugin_id)

    def bind_application_plugins(
        self,
        channel: Any,
        *,
        agent_client: Any = None,
        media_attachment_normalizer: Callable[[dict[str, Any], str | None], Awaitable[None]]
        | None = None,
    ) -> None:
        from agent_gateway.extensions.sdk.application_plugin import (
            ApplicationPluginServices,
        )

        services = ApplicationPluginServices(
            agent_client=agent_client,
            media_attachment_normalizer=media_attachment_normalizer,
        )
        for plugin in self.get_application_plugins():
            plugin.bind_web_channel(_ApplicationPluginChannel(channel, plugin), services)
        channel.application_plugin_registry = self

    def get_agent_server_client_extension(self) -> AgentServerClientExtension | None:
        return self._agent_server_client

    def get_agent_server_client(self) -> AgentServerClient | None:
        ext = self._agent_server_client
        return ext.get_client() if ext is not None else None

    def get_crypto_utility_extension(self) -> CryptoUtility | None:
        return self._crypto_tool

    def get_crypto_provider(self) -> CryptoProvider | None:
        ext = self._crypto_tool
        return ext.get_crypto() if ext is not None else None

    def get_third_agent_extension(self) -> ThirdAgentExtension | None:
        return self._third_agent

    def get_third_agent(self) -> ThirdAgent | None:
        """Return registered ThirdAgent, or None when no extension registered."""
        ext = self._third_agent
        return ext.get_third_agent() if ext is not None else None

    def register(
        self,
        event: str,
        handler: Callable,
        priority: int = 100,
        **kwargs,
    ) -> None:
        if self._closed:
            raise RuntimeError("Extension registry is closed")
        if self._callback_framework is None:
            self._pending_callbacks.append(("register", event, handler, priority, kwargs))
        else:
            self._callback_framework.register_sync(event, handler, priority=priority, **kwargs)

    def unregister(self, event: str, handler: Callable | None = None) -> None:
        if self._callback_framework is None:
            if not self._closed:
                self._pending_callbacks.append(("unregister", event, handler, 100, {}))
        else:
            unregister_callback_sync(self._callback_framework, event, handler)

    async def trigger(self, event: str, context: Any | None = None, **kwargs: Any) -> None:
        """触发事件。约定由调用方传入的 context 承载回调副作用"""
        if self._callback_framework is None and not self._pending_callbacks and self._callback_factory is not None:
            # No extension registered a callback: an empty trigger needs no SDK.
            if self._closed:
                raise RuntimeError("Extension registry is closed")
            return
        await self.prepare_callback_framework()
        if context is None and not kwargs:
            await self.callback_framework.trigger(event)
        elif context is not None:
            await self.callback_framework.trigger(event, context, **kwargs)
        else:
            await self.callback_framework.trigger(event, **kwargs)

    @property
    def config(self) -> ExtensionConfig:
        return self._config
