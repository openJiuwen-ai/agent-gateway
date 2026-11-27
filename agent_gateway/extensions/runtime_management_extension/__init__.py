__all__ = ["register_extensions"]


# Defer registration loading until the extension is requested.
def __getattr__(name):
    if name == "register_extensions":
        from agent_gateway.extensions.runtime_management_extension.extension import register_extensions

        return register_extensions
    raise AttributeError(name)
