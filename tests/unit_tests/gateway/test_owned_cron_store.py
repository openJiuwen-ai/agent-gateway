"""A slot view must never mutate another owner's record in the shared file."""

import asyncio
import json

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.store import FileCronJobStore


def view(path, slot):
    from agent_gateway.cron.ownership import CronOwnership
    from agent_gateway.cron.owned_store import OwnedCronJobStore
    return OwnedCronJobStore(FileCronJobStore(path), CronOwnership(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy()), slot=slot)


def create(store, user='alice', job_id='a', **changes):
    args = dict(job_id=job_id, user_id=user, name='task', cron_expr='0 0 8 * * ? *',
                timezone='UTC', description='do work', targets='web')
    args.update(changes)
    return store.create_job(**args)


@pytest.mark.asyncio
async def test_list_get_and_foreign_mutations_are_slot_scoped(tmp_path):
    path = tmp_path / 'jobs.json'
    a, b = view(path, 3), view(path, 2)
    await create(a)
    await create(b, 'bob', 'b')
    assert [j.id for j in await a.list_jobs(strict=True)] == ['a']
    assert [j.id for j in await b.list_jobs()] == ['b']
    assert await a.get_job('b') is None
    with pytest.raises(KeyError):
        await a.update_job('b', {'enabled': False})
    assert await a.delete_job('b', force=True) is False
    assert (await b.get_job('b')).enabled
    assert a.path == path and a.supports_watch is False
    assert await a.get_revision() > 0


@pytest.mark.asyncio
@pytest.mark.parametrize('changes', [{'user_id': 'bob'}, {'group_id': 'g'}, {'bot_id': 'b'}, {'id': 'b'}, {'job_id': 'b'}])
async def test_update_cannot_change_persisted_owner_or_identifier(tmp_path, changes):
    store = view(tmp_path / 'jobs.json', 3)
    await create(store)
    before = store.path.read_bytes()
    with pytest.raises(ValueError):
        await store.update_job('a', changes)
    assert store.path.read_bytes() == before


@pytest.mark.asyncio
async def test_wrong_slot_and_noncanonical_create_never_write(tmp_path):
    store = view(tmp_path / 'jobs.json', 2)
    for user in ('alice', ' bob ', '', 'default'):
        with pytest.raises(ValueError):
            await create(store, user=user)
    assert not store.path.exists()


@pytest.mark.asyncio
async def test_duplicate_create_is_exclusive_even_when_other_owner_has_that_id(tmp_path):
    path = tmp_path / 'jobs.json'
    await create(view(path, 3))
    before = path.read_bytes()
    with pytest.raises(ValueError):
        await create(view(path, 2), user='bob')
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_disabled_unsupported_task_remains_visible_but_cannot_enable(tmp_path):
    store = view(tmp_path / 'jobs.json', 3)
    await create(store, enabled=False, targets='feishu', credential_ref='account-ref')
    assert [j.id for j in await store.list_jobs()] == ['a']
    with pytest.raises(ValueError):
        await store.update_job('a', {'enabled': True})
    assert not (await store.get_job('a')).enabled
    with pytest.raises(ValueError):
        await create(store, job_id='c', credential_ref='account-ref')


@pytest.mark.asyncio
async def test_global_project_disable_requires_future_coordinator(tmp_path):
    store = view(tmp_path / 'jobs.json', 3)
    await create(store, project_id='shared')
    with pytest.raises(NotImplementedError):
        await store.disable_project_jobs('shared')
    assert (await store.get_job('a')).enabled


@pytest.mark.asyncio
async def test_shared_file_updates_preserve_other_slots_and_legacy_reader(tmp_path):
    path = tmp_path / 'jobs.json'
    a, b = view(path, 3), view(path, 2)
    await asyncio.gather(create(a), create(b, 'bob', 'b'))
    await asyncio.gather(a.update_job('a', {'last_session_id': 's1'}), b.update_job('b', {'enabled': False}))
    raw = json.loads(path.read_text())
    assert raw['version'] == 1
    records = {j['id']: j for j in raw['jobs']}
    assert records['a']['last_session_id'] == 's1'
    assert records['b']['enabled'] is False
    assert {j.id for j in await FileCronJobStore(path).list_jobs(strict=True)} == {'a', 'b'}
    assert await a.delete_job('a') is True
    assert [j.id for j in await b.list_jobs()] == ['b']


@pytest.mark.asyncio
async def test_mutation_rechecks_owner_under_the_same_file_lock(tmp_path):
    """Replace after the view's read but before the locked mutation, without timing sleeps."""
    path = tmp_path / 'jobs.json'
    store = view(path, 3)
    await create(store)
    backend = store.backend
    original = backend._run_locked
    replaced = False

    async def replace_before_lock(fn):
        nonlocal replaced
        if not replaced:
            replaced = True
            raw = json.loads(path.read_text())
            raw['jobs'][0]['user_id'] = 'bob'
            path.write_text(json.dumps(raw))
        return await original(fn)

    backend._run_locked = replace_before_lock
    with pytest.raises(KeyError):
        await store.update_job('a', {'description': 'wrong user'})
    assert json.loads(path.read_text())['jobs'][0]['description'] == 'do work'


@pytest.mark.asyncio
async def test_delete_also_checks_replaced_owner_under_lock(tmp_path):
    path = tmp_path / 'jobs.json'
    store = view(path, 3)
    await create(store)
    original = store.backend._run_locked

    async def replace_before_lock(fn):
        raw = json.loads(path.read_text())
        raw['jobs'][0]['user_id'] = 'bob'
        path.write_text(json.dumps(raw))
        return await original(fn)

    store.backend._run_locked = replace_before_lock
    assert await store.delete_job('a', force=True) is False
    assert json.loads(path.read_text())['jobs'][0]['user_id'] == 'bob'


@pytest.mark.asyncio
async def test_same_slot_is_not_user_authorization(tmp_path):
    """Two distinct users sharing a slot remain visible to the original controller."""
    store = view(tmp_path / 'jobs.json', 1)
    await create(store, 'local-default', 'a')
    await create(store, '张三', 'b')
    assert {job.user_id for job in await store.list_jobs()} == {'local-default', '张三'}


@pytest.mark.asyncio
async def test_unsupported_disabled_job_cannot_be_run_by_capability_check(tmp_path):
    from agent_gateway.cron.preflight import require_supported_cron
    store = view(tmp_path / 'jobs.json', 3)
    job = await create(store, enabled=False, targets='feishu')
    with pytest.raises(ValueError, match='capability'):
        require_supported_cron(job)


def test_real_processes_preserve_all_slots_in_one_legacy_file(tmp_path):
    """Independent interpreters contend on the real sidecar file lock."""
    import os
    import subprocess
    import sys

    script = '''
import asyncio, sys
from pathlib import Path
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.cron.owned_store import OwnedCronJobStore
from agent_gateway.cron.store import FileCronJobStore
async def main():
    path, slot, user = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    store = OwnedCronJobStore(FileCronJobStore(path), CronOwnership(WorkerManifest((0,1,2,3)), LocalIdentityPolicy()), slot=slot)
    print('ready', flush=True)
    await asyncio.to_thread(sys.stdin.readline)
    for i in range(8):
        job_id = f'{slot}-{i}'
        await store.create_job(job_id=job_id, user_id=user, name='task', cron_expr='0 0 8 * * ? *', timezone='UTC', description='process test', targets='web')
        await store.update_job(job_id, {'last_session_id': f's-{slot}-{i}'})
        await store.update_job(job_id, {'enabled': False})
asyncio.run(main())
'''
    path = tmp_path / 'jobs.json'
    processes = []
    try:
        for slot, user in [(3, 'alice'), (2, 'bob'), (1, 'local-default')]:
            processes.append(subprocess.Popen(
                [sys.executable, '-c', script, str(path), str(slot), user],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=dict(os.environ),
            ))
        for process in processes:
            process.stdin.write('go\n')
            process.stdin.flush()
        for process in processes:
            stdout, stderr = process.communicate(timeout=25)
            assert process.returncode == 0, stderr
            assert 'ready' in stdout
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
    records = json.loads(path.read_text())['jobs']
    assert len(records) == 24
    assert {job['id'] for job in records} == {f'{s}-{i}' for s in (1, 2, 3) for i in range(8)}
    for job in records:
        assert job['last_session_id'] == 's-' + job['id']
        assert job['enabled'] is False


@pytest.mark.asyncio
@pytest.mark.parametrize('payload', ['', '{}', '{"version":2,"jobs":[]}'])
async def test_create_must_not_repair_or_overwrite_corrupt_inventory(tmp_path, payload):
    path = tmp_path / 'jobs.json'
    path.write_text(payload)
    with pytest.raises(ValueError, match='snapshot'):
        await create(view(path, 3))
    assert path.read_text() == payload


@pytest.mark.asyncio
async def test_mutating_one_slot_does_not_migrate_other_slots_records(tmp_path):
    path = tmp_path / 'jobs.json'
    await create(view(path, 3))
    await create(view(path, 2), 'bob', 'b', targets='tui')
    raw = json.loads(path.read_text())
    for item in raw['jobs']:
        item.pop('work_mode')
    path.write_text(json.dumps(raw))
    other_before = next(item for item in raw['jobs'] if item['id'] == 'b')
    await view(path, 3).update_job('a', {'last_session_id': 'done'})
    other_after = next(item for item in json.loads(path.read_text())['jobs'] if item['id'] == 'b')
    assert other_after == other_before
    # When the target is updated, its inferred legacy work-mode is retained.
    await view(path, 2).update_job('b', {'last_session_id': 'done-b'})
    assert (await view(path, 2).get_job('b')).work_mode == 'code'
