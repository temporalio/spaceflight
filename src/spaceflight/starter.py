import asyncio

from temporalio.api.enums.v1 import WorkflowIdReusePolicy
from temporalio.client import Client

from spaceflight.space_worker import SpaceWorkflow


async def main():
    client = await Client.connect("localhost:7233")

    await client.start_workflow(
        SpaceWorkflow.run,
        id="space-workflow",
        task_queue="temporal-in-space",
        id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
    )


if __name__ == "__main__":
    asyncio.run(main())
