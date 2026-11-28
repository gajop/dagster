import os
from collections.abc import Iterator
from functools import partial

from dagster_shared.utils.timing import format_duration

import dagster._check as check
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.api import ExecuteRunWithPlanIterable
from dagster._core.execution.context.system import PlanExecutionContext, PlanOrchestrationContext
from dagster._core.execution.context_creation_job import PlanExecutionContextManager
from dagster._core.execution.plan.execute_plan import inner_plan_execution_iterator
from dagster._core.execution.plan.instance_concurrency_context import InstanceConcurrencyContext
from dagster._core.execution.plan.plan import ExecutionPlan
from dagster._core.execution.retries import RetryMode
from dagster._core.execution.step_dependency_config import StepDependencyConfig
from dagster._core.executor.base import Executor
from dagster._utils.timing import time_execution_scope


def inprocess_execution_iterator(
    job_context: PlanExecutionContext,
    execution_plan: ExecutionPlan,
    instance_concurrency_context: InstanceConcurrencyContext | None = None,
    *,
    max_concurrent: int | None = None,
) -> Iterator[DagsterEvent]:
    with InstanceConcurrencyContext(
        job_context.instance, job_context.dagster_run
    ) as instance_concurrency_context:
        try:
            yield from inner_plan_execution_iterator(
                job_context,
                execution_plan,
                instance_concurrency_context,
                max_concurrent=max_concurrent,
            )
        finally:
            if max_concurrent is not None:
                # The threaded iterator joins its workers before exiting, even on interruption.
                instance_concurrency_context.free_claimed_steps()


class InProcessExecutor(Executor):
    def __init__(
        self,
        retries: RetryMode,
        step_dependency_config: StepDependencyConfig = StepDependencyConfig.default(),
        marker_to_close: str | None = None,
        *,
        max_concurrent: int | None = None,
    ):
        self._retries = check.inst_param(retries, "retries", RetryMode)
        self._step_dependency_config = check.inst_param(
            step_dependency_config, "step_dependency_config", StepDependencyConfig
        )
        self.marker_to_close = check.opt_str_param(marker_to_close, "marker_to_close")
        self._max_concurrent = max_concurrent

    @property
    def retries(self) -> RetryMode:
        return self._retries

    @property
    def step_dependency_config(self) -> StepDependencyConfig:
        return self._step_dependency_config

    def execute(
        self, plan_context: PlanOrchestrationContext, execution_plan: ExecutionPlan
    ) -> Iterator[DagsterEvent]:
        check.inst_param(plan_context, "plan_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        step_keys_to_execute = execution_plan.step_keys_to_execute
        mode = "in process" if self._max_concurrent is None else "in process on threads"

        yield DagsterEvent.engine_event(
            plan_context,
            f"Executing steps {mode} (pid: {os.getpid()})",
            event_specific_data=EngineEventData.in_process(os.getpid(), step_keys_to_execute),
        )

        with time_execution_scope() as timer_result:
            yield from iter(
                ExecuteRunWithPlanIterable(
                    execution_plan=plan_context.execution_plan,
                    iterator=partial(
                        inprocess_execution_iterator, max_concurrent=self._max_concurrent
                    ),
                    execution_context_manager=PlanExecutionContextManager(
                        job=plan_context.job,
                        retry_mode=plan_context.retry_mode,
                        execution_plan=plan_context.execution_plan,
                        run_config=plan_context.run_config,
                        dagster_run=plan_context.dagster_run,
                        instance=plan_context.instance,
                        raise_on_error=plan_context.raise_on_error,
                        output_capture=plan_context.output_capture,
                        step_dependency_config=self.step_dependency_config,
                    ),
                )
            )

        yield DagsterEvent.engine_event(
            plan_context,
            f"Finished steps {mode} (pid: {os.getpid()}) in {format_duration(timer_result.millis)}",
            event_specific_data=EngineEventData.in_process(os.getpid(), step_keys_to_execute),
        )
