"""Read-only inventory must not migrate data or hide unsupported jobs."""

import json

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.store import FileCronJobStore


def raw_job(job_id='j1', **changes):
    item = dict(id=job_id, name='task', enabled=True, cron_expr='0 0 8 * * ? *',
                timezone='UTC', description='private task text', targets='web', user_id='alice')
    item.update(changes)
    return item


def write_store(path, jobs, version=1):
    path.write_text(json.dumps(dict(version=version, jobs=jobs)), encoding='utf-8')
    return FileCronJobStore(path)


def ownership():
    from agent_gateway.cron.ownership import CronOwnership
    return CronOwnership(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())


@pytest.mark.asyncio
async def test_snapshot_infers_legacy_work_mode_in_memory_without_any_filesystem_write(tmp_path):
    path = tmp_path / 'jobs.json'
    store = write_store(path, [raw_job(targets=[{'channel_id': 'tui'}])])
    before = path.read_bytes(), path.stat().st_mtime_ns
    snapshot = await store.read_snapshot()
    assert snapshot[0].work_mode == 'code'
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ['jobs.json']


@pytest.mark.asyncio
async def test_missing_snapshot_does_not_create_parent_or_lock(tmp_path):
    path = tmp_path / 'missing' / 'jobs.json'
    assert await FileCronJobStore(path).read_snapshot() == []
    assert not path.parent.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('payload', [
    'not json', '[]', '', '{"version":2,"jobs":[]}', '{"version":true,"jobs":[]}',
    '{"version":1,"jobs":{}}', '{"version":1,"jobs":[null]}',
])
async def test_corrupt_or_unknown_schema_is_reported_without_repair(tmp_path, payload):
    path = tmp_path / 'jobs.json'
    path.write_text(payload)
    with pytest.raises(ValueError, match='snapshot'):
        await FileCronJobStore(path).read_snapshot()
    assert path.read_text() == payload
    assert not path.with_suffix('.json.lock').exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [
    {'user_id': ' alice '}, {'user_id': 7}, {'group_id': None}, {'bot_id': ' bot '},
    {'targets': 'not-a-channel'}, {'enabled': 'false'}, {'credential_ref': 7},
    {'description': ''}, {'id': ' j1 '},
])
async def test_snapshot_rejects_records_that_old_parser_would_normalize_or_skip(tmp_path, change):
    path = tmp_path / 'jobs.json'
    store = write_store(path, [raw_job(**change)])
    before = path.read_bytes()
    with pytest.raises(ValueError, match='snapshot'):
        await store.read_snapshot()
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_duplicate_ids_cannot_hide_a_second_owner(tmp_path):
    store = write_store(tmp_path / 'jobs.json', [raw_job(), raw_job(user_id='bob')])
    with pytest.raises(ValueError, match='snapshot'):
        await store.read_snapshot()


@pytest.mark.asyncio
async def test_inventory_counts_slots_and_reports_only_safe_identifiers(tmp_path):
    from agent_gateway.cron.preflight import inspect_cron_store
    store = write_store(tmp_path / 'jobs.json', [raw_job(), raw_job('j2', user_id='bob')])
    report = await inspect_cron_store(store, ownership())
    assert report.ready and report.total == 2
    assert report.slot_counts == ((0, 0), (1, 0), (2, 1), (3, 1))
    assert report.issues == ()
    assert 'private task text' not in repr(report)


@pytest.mark.asyncio
@pytest.mark.parametrize(('changes', 'reason'), [
    ({'user_id': ''}, 'legacy_owner'), ({'user_id': 'default'}, 'legacy_owner'),
    ({'mode': 'proactive.tick'}, 'proactive'),
    ({'id': 'proactive-tick-auto'}, 'proactive'),
    ({'credential_ref': 'secret-ref'}, 'credential_ref'),
    ({'targets': 'feishu'}, 'target'),
])
@pytest.mark.parametrize('enabled', [True, False])
async def test_unsupported_enabled_blocks_disabled_is_retained_with_warning(tmp_path, changes, reason, enabled):
    from agent_gateway.cron.preflight import inspect_cron_store
    store = write_store(tmp_path / 'jobs.json', [raw_job(**changes, enabled=enabled)])
    before = store.path.read_bytes()
    report = await inspect_cron_store(store, ownership())
    assert report.ready is (not enabled)
    assert report.total == 1
    issue = next(issue for issue in report.issues if issue.reason == reason)
    assert issue.blocking is enabled
    assert 'secret-ref' not in repr(report)
    assert 'private task text' not in repr(report)
    assert store.path.read_bytes() == before


@pytest.mark.asyncio
async def test_enabled_proactive_config_blocks_even_without_generated_record(tmp_path):
    from agent_gateway.cron.preflight import inspect_cron_store
    report = await inspect_cron_store(FileCronJobStore(tmp_path / 'absent.json'), ownership(), proactive_enabled=True)
    assert not report.ready
    assert report.issues[0].reason == 'proactive_config'


@pytest.mark.asyncio
async def test_invalid_utf8_is_sanitized_without_echoing_bytes(tmp_path):
    path = tmp_path / 'jobs.json'
    path.write_bytes(b'{"private-key":"\xff"}')
    with pytest.raises(ValueError, match='snapshot'):
        await FileCronJobStore(path).read_snapshot()


@pytest.mark.asyncio
async def test_legacy_versionless_file_and_disabled_default_are_preserved(tmp_path):
    from agent_gateway.cron.preflight import inspect_cron_store
    path = tmp_path / 'jobs.json'
    path.write_text(json.dumps({'jobs': [raw_job(user_id='default', enabled=False)]}))
    before = path.read_bytes()
    report = await inspect_cron_store(FileCronJobStore(path), ownership())
    assert report.ready and report.total == 1
    assert sum(count for _, count in report.slot_counts) == 0
    assert path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('changes', [
    {'targets': [{'channel_id': 'web'}, {'channel_id': 'feishu'}]},
    {'model_selection': 'broken-selection'}, {'work_mode': 'invalid'},
    {'wake_offset_seconds': -10}, {'mode': 'not-a-mode'},
])
async def test_preflight_cannot_hide_unsupported_targets_or_corrupt_execution_fields(tmp_path, changes):
    from agent_gateway.cron.preflight import inspect_cron_store
    store = write_store(tmp_path / 'jobs.json', [raw_job(**changes)])
    with pytest.raises(ValueError, match='snapshot'):
        await inspect_cron_store(store, ownership())
