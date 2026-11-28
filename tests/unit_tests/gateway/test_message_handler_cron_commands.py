"""Controlled AS Cron ingress and original execution-context hooks."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.message_handler.message_handler import MessageHandler
from tests.unit_tests.gateway.test_message_handler_cron_push import _bare_handler, _CapturingCronController, _cron_push_wire


@pytest.mark.asyncio
async def test_controlled_cron_push_bypasses_ordered_body_path_and_uses_wire_owner():
    handler = _bare_handler(object(), _CapturingCronController())
    commands = SimpleNamespace(handle_push=AsyncMock())
    handler.set_cron_command_runtime(commands)
    wire = _cron_push_wire(
        request_id='req-controlled',
        body={'action': 'list', 'command_id': 'cmd-controlled', 'data': {}},
        metadata={'_jiuwenswarm_cron_owner_user_id': 'alice', 'auth_session': 'untrusted'},
    )
    handler._handle_agent_server_push_ordered = AsyncMock()
    await handler._handle_agent_server_push(wire)
    commands.handle_push.assert_awaited_once()
    args = commands.handle_push.call_args.kwargs
    assert args['request_id'] == 'req-controlled'
    assert args['metadata']['_jiuwenswarm_cron_owner_user_id'] == 'alice'
    handler._handle_agent_server_push_ordered.assert_not_awaited()


@pytest.mark.asyncio
async def test_owner_executor_uses_explicit_trusted_context_even_after_stream_cleanup():
    controller = _CapturingCronController()
    handler = _bare_handler(object(), controller)
    handler._stream_metadata['req-1'] = {'auth_session': 'wrong-live-context'}
    await handler._handle_cron_push_payload(
        payload={'action': 'create', 'data': {'name': 'task', 'mode': 'team'}},
        request_id='req-1', channel_id='', session_id='sess-1',
        metadata={'auth_session': 'untrusted'}, user_id='alice',
        _trusted_cron_metadata={'auth_session': 'original-login', 'mode': 'agent'},
    )
    assert controller.create_params['_auth_session'] == 'original-login'
    assert controller.create_params['mode'] == 'agent'
    assert controller.create_params['user_id'] == 'alice'


@pytest.mark.asyncio
async def test_explicit_empty_context_never_falls_back_to_unrelated_stream_login():
    controller = _CapturingCronController()
    handler = _bare_handler(object(), controller)
    handler._stream_metadata['req-1'] = {'auth_session': 'wrong-live-context'}
    await handler._handle_cron_push_payload(
        payload={'action': 'create', 'data': {'name': 'task'}},
        request_id='req-1', channel_id='', session_id='sess-1',
        metadata=None, user_id='alice', _trusted_cron_metadata={},
    )
    assert '_auth_session' not in controller.create_params


@pytest.mark.asyncio
async def test_non_stream_send_registers_original_context_before_as_call():
    handler = _bare_handler(SimpleNamespace(send_request=AsyncMock(return_value='ok')), object())
    commands = SimpleNamespace(remember=Mock(), complete=Mock())
    handler.set_cron_command_runtime(commands)
    handler._apply_session_login_owner = AsyncMock()
    env = SimpleNamespace(request_id='req1', channel_context={'auth_session': 'original'})
    result = await handler._send_non_stream_agent_request(env)
    assert result == 'ok'
    commands.remember.assert_called_once_with(env, {'auth_session': 'original'})


def test_stream_cleanup_retains_cron_context_before_removing_original_tracking():
    handler = _bare_handler(object(), object())
    commands = SimpleNamespace(remember=Mock(), complete=Mock())
    handler.set_cron_command_runtime(commands)
    handler._scheduler_stream_ids = set()
    handler._stream_tasks = {}
    handler._stream_channels = {}
    handler._stream_team_ids = {}
    handler._stream_processing_finished = set()
    handler._stream_emits_processing_status = {}
    handler._stream_methods = {}
    handler._stream_app_ids = {}
    handler._stream_modes = {}
    handler._pop_stream_tracking('req1')
    commands.complete.assert_called_once_with('req1')


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['command', 'run_now'])
async def test_controlled_cron_ack_has_a_bounded_wait(monkeypatch, operation):
    handler = _bare_handler(object(), object())
    handler.set_cron_command_runtime(SimpleNamespace())
    send = AsyncMock()
    monkeypatch.setattr('agent_gateway.message_handler.message_handler.send_agent_request_with_timeout', send)
    if operation == 'command':
        await handler._push_cron_command_ack(command_id='c1', data={'status': 'ok'})
    else:
        await handler._push_cron_run_now_ack(request_id='r1', job_id='j1', run_id='run1')
    assert 0 < send.call_args.kwargs['timeout_seconds'] <= 2


@pytest.mark.asyncio
async def test_stream_registers_outer_approval_request_before_as_consumption():
    handler = _bare_handler(object(), object())
    commands = SimpleNamespace(remember=Mock(), complete=Mock())
    handler.set_cron_command_runtime(commands)
    handler._apply_session_login_owner = AsyncMock()
    handler._stream_app_ids = {}
    handler._expert_stream_route = Mock(return_value={})
    handler._is_permission_resume = Mock(return_value=False)
    env = SimpleNamespace(request_id='approval-R1', channel='web', user_id='alice',
                          session_id='real-session', params={'request_id': 'old-R0', 'mode': 'agent'})
    registered = False
    async def stream(request):
        commands.remember.assert_called_once_with(env, {'auth_session': 'original'})
        nonlocal registered
        registered = True
        raise asyncio.CancelledError()
        yield
    handler._evolution_approval = SimpleNamespace(clear_session_in_progress=Mock())
    handler._stream_modes = {}
    handler._active_chat_tasks = {}
    handler.agent_client = SimpleNamespace(send_request_stream=stream)
    handler._cron_permission_resume_settled = Mock(return_value=False)
    handler._pop_stream_tracking_and_broadcast = AsyncMock()
    handler._session_has_streams_blocking_processing_false = Mock(return_value=False)
    handler._send_processing_status = AsyncMock()
    await handler.process_stream(env, 'real-session', {'auth_session': 'original'}, emit_processing_status=False)
    assert registered
