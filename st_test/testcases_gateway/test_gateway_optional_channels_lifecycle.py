from __future__ import annotations

import json
from pathlib import Path
import shlex
import sys

from testcases_support.process import start_gateway


def test_entrypoint_retains_optional_channel_stage_order(tmp_path, monkeypatch, stub_agent_server):
    events_file = tmp_path / 'lifecycle.jsonl'
    stop_file = tmp_path / 'stop-requested'
    program = '''
import asyncio
import json
from pathlib import Path
from agent_gateway.application import single
from agent_gateway.application.optional_channels import OptionalChannels
from agent_gateway.channel_manager.web.web_connect import WebChannel
from agent_gateway.routing.protocol_runtime import ProtocolRuntime

events_file = Path(EVENTS_FILE)
stop_file = Path(STOP_FILE)

# Exercise the same shutdown Event as the signal handler on every OS.
# Windows SIGTERM force-terminates a process and cannot verify finally blocks.
original_run_single = single.run_single
async def run_until_stop(**kwargs):
    async def watch_stop():
        while not stop_file.exists():
            await asyncio.sleep(.05)
        kwargs['shutdown_event'].set()
    watcher = asyncio.create_task(watch_stop())
    try:
        return await original_run_single(**kwargs)
    finally:
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
single.run_single = run_until_stop

def record(event):
    with events_file.open('a') as output:
        output.write(json.dumps(event) + '\\n')

def wrap_sync(owner, name, event):
    original = getattr(owner, name)
    def invoke(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        record(event)
        return result
    setattr(owner, name, invoke)

def wrap_async(owner, name, event):
    original = getattr(owner, name)
    async def invoke(self, *args, **kwargs):
        result = await original(self, *args, **kwargs)
        record(event)
        return result
    setattr(owner, name, invoke)

for name in ('start_a2a', 'install', 'schedule_initial_configuration'):
    wrap_sync(OptionalChannels, name, name)
for name in ('stop_configuration', 'stop_a2a', 'stop_channels'):
    wrap_async(OptionalChannels, name, name)
wrap_async(WebChannel, 'wait_ready', 'web.ready')
wrap_async(WebChannel, 'stop', 'web.stop')
wrap_async(ProtocolRuntime, 'start', 'protocol.ready')
wrap_async(ProtocolRuntime, 'stop', 'protocol.stop')
from agent_gateway.app_gateway import main
main()
'''.replace('EVENTS_FILE', repr(str(events_file))).replace('STOP_FILE', repr(str(stop_file)))
    repository = Path(__file__).resolve().parents[2]
    monkeypatch.setenv('ST_GATEWAY_CONFIG_SRC', str(repository / 'agent_gateway/resources/config.yaml'))
    monkeypatch.setenv('AGENT_GATEWAY_BIN', shlex.join([sys.executable, '-c', program]))
    gateway = start_gateway(tmp_path / 'gateway', stub_agent_server.url)
    try:
        assert gateway.proc.poll() is None
        if sys.platform == 'win32':
            stop_file.touch()
            assert gateway.proc.wait(timeout=15) == 0
    finally:
        gateway.stop()
    events = [json.loads(line) for line in events_file.read_text().splitlines()]
    expected = [
        'web.ready', 'start_a2a', 'install', 'protocol.ready',
        'schedule_initial_configuration', 'stop_configuration', 'stop_a2a',
        'protocol.stop', 'web.stop', 'stop_channels',
    ]
    assert events == expected
