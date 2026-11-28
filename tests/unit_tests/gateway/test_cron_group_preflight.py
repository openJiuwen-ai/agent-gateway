"""Control refuses unsupported Cron before starting any Worker."""

import pytest
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.cron.store import FileCronJobStore


@pytest.mark.asyncio
async def test_control_preflight_is_readonly_and_rejects_proactive_config(tmp_path):
    from agent_gateway.application.cron_preflight import preflight_controlled_cron
    path = tmp_path / 'uncreated' / 'jobs.json'
    ownership = CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy())
    with pytest.raises(ValueError, match='preflight'):
        await preflight_controlled_cron(
            ownership, config={'proactive_recommendation': {'enabled': True}},
            store=FileCronJobStore(path=path),
        )
    assert not path.parent.exists()


@pytest.mark.asyncio
async def test_control_preflight_rejects_enabled_legacy_without_rewriting(tmp_path):
    from agent_gateway.application.cron_preflight import preflight_controlled_cron
    store = FileCronJobStore(path=tmp_path / 'jobs.json')
    await store.create_job(name='legacy', cron_expr='0 9 * * *', timezone='UTC',
                           description='private body', targets='web', user_id='')
    before = store.path.read_bytes()
    with pytest.raises(ValueError, match='preflight') as failure:
        await preflight_controlled_cron(
            CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy()),
            config={}, store=store,
        )
    assert 'private body' not in str(failure.value)
    assert store.path.read_bytes() == before


@pytest.mark.asyncio
async def test_control_preflight_rejects_nonfile_backend(tmp_path):
    from agent_gateway.application.cron_preflight import preflight_controlled_cron
    with pytest.raises(ValueError, match='file'):
        await preflight_controlled_cron(
            CronOwnership(WorkerManifest((0,)), LocalIdentityPolicy()),
            config={'gateway': {'cron': {'store_backend': 'etcd'}}},
            store=FileCronJobStore(path=tmp_path / 'jobs.json'),
        )
