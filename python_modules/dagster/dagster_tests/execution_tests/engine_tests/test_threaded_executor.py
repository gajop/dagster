import asyncio
import os
import threading

import dagster as dg
import pytest
from dagster._core.execution.plan.active import ActiveExecution
from dagster._core.system_config.objects import ResolvedRunConfig


@dg.resource
def thread_barrier():
    return threading.Barrier(2, timeout=10)


@dg.op(required_resource_keys={"thread_barrier"})
def thread_identity(context: dg.OpExecutionContext):
    pid = os.getpid()
    thread = threading.get_ident()
    context.resources.thread_barrier.wait()
    return pid, thread


@dg.op
def check_thread_identities(first, second, third, fourth):
    identities = [first, second, third, fourth]
    assert {pid for pid, _ in identities} == {os.getpid()}
    assert len({thread for _, thread in identities}) == 2


@dg.job(resource_defs={"thread_barrier": thread_barrier, "io_manager": dg.mem_io_manager})
def parallel_job():
    check_thread_identities(
        thread_identity.alias("first")(),
        thread_identity.alias("second")(),
        thread_identity.alias("third")(),
        thread_identity.alias("fourth")(),
    )


@pytest.mark.parametrize(
    ("instance_config", "run_config"),
    [
        ({}, {"execution": {"config": {"threaded": {"max_concurrent": 2}}}}),
        ({"default_executor": {"threaded": {"max_concurrent": 2}}}, {}),
        (
            {"default_executor": {"threaded": {"max_concurrent": 4}}},
            {"execution": {"config": {"threaded": {"max_concurrent": 2}}}},
        ),
    ],
    ids=["run-config", "instance-default", "run-config-override"],
)
def test_threads_execute_only_ready_steps_and_respect_max_concurrent(instance_config, run_config):
    with dg.instance_for_test(overrides=instance_config) as instance:
        result = dg.execute_job(
            dg.reconstructable(parallel_job),
            instance=instance,
            run_config=run_config,
        )
        assert result.success


def test_instance_default_respects_executor_specified_on_job():
    @dg.job(executor_def=dg.in_process_executor)
    def serial_job():
        pass

    config = ResolvedRunConfig.build(
        serial_job, default_executor={"threaded": {"max_concurrent": 2}}
    )
    assert config.execution.execution_engine_name == "in_process"


def test_threaded_mode_requires_an_explicit_limit():
    with pytest.raises(dg.DagsterInvalidConfigError, match="max_concurrent"):
        dg.validate_run_config(parallel_job, {"execution": {"config": {"threaded": {}}}})


@dg.op(out=dg.DynamicOut())
def emit_dynamic_outputs():
    yield dg.DynamicOutput(1, mapping_key="one")
    yield dg.DynamicOutput(2, mapping_key="two")


@dg.op
async def double_after_retry(context: dg.OpExecutionContext, number: int) -> int:
    await asyncio.sleep(0.01)
    if context.retry_number == 0:
        raise dg.RetryRequested(max_retries=1)
    return number * 2


@dg.op
def check_collected_results(results: list[int]):
    assert sorted(results) == [2, 4]


@dg.job(executor_def=dg.threaded_executor)
def retry_dynamic_job():
    check_collected_results(emit_dynamic_outputs().map(double_after_retry).collect())


def test_threads_support_dynamic_steps_async_ops_and_retries():
    with dg.instance_for_test() as instance:
        result = dg.execute_job(
            dg.reconstructable(retry_dynamic_job),
            instance=instance,
            run_config={"execution": {"config": {"max_concurrent": 2}}},
        )
        assert result.success
        retries = {event.step_key for event in result.all_events if event.is_step_up_for_retry}
        assert retries == {"double_after_retry[one]", "double_after_retry[two]"}


thread_started = threading.Event()
release_thread = threading.Event()


@dg.op(pool="threaded_test_pool")
def wait_for_interrupt():
    thread_started.set()
    assert release_thread.wait(timeout=10)


@dg.op
def must_not_run(_upstream):
    pytest.fail("A downstream step ran after the executor was interrupted")


@dg.job(executor_def=dg.threaded_executor)
def interrupted_job():
    must_not_run(wait_for_interrupt())


def test_interrupt_stops_dispatching_new_steps_and_releases_pool_slots(
    monkeypatch: pytest.MonkeyPatch, instance: dg.DagsterInstance
):
    thread_started.clear()
    release_thread.clear()

    def interrupted(_active_execution):
        if not thread_started.is_set():
            return False
        release_thread.set()
        return True

    monkeypatch.setattr(ActiveExecution, "check_for_interrupts", interrupted)
    instance.event_log_storage.set_concurrency_slots("threaded_test_pool", 1)
    result = dg.execute_job(
        dg.reconstructable(interrupted_job),
        instance=instance,
        run_config={"execution": {"config": {"max_concurrent": 2}}},
    )
    assert not result.success
    started = instance.all_logs(result.run_id, of_type=dg.DagsterEventType.STEP_START)
    assert [event.step_key for event in started] == ["wait_for_interrupt"]
    pool = instance.event_log_storage.get_concurrency_info("threaded_test_pool")
    assert pool.active_slot_count == 0
