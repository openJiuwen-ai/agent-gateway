__all__ = ["register_extensions"]


# extension imports delegated_router, which imports this package's authorization
# module. Defer registration loading so either module can be imported first.
def __getattr__(name):
    if name == "register_extensions":
        from agent_gateway.extensions.runtime_management_extension.extension import register_extensions

        return register_extensions
    raise AttributeError(name)
