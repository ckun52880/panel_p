"""
Periodic Full User Sync Job

This job periodically syncs ALL active users to ALL nodes to ensure consistency.
This helps catch any missed updates due to:
- Temporary node failures
- Network issues
- Race conditions during user status changes
- Queue failures

The job runs less frequently than normal operations to avoid overhead,
but provides a safety net for data consistency.
"""

import asyncio

from PasarGuardNodeBridge import Health

from app import scheduler
from app.db import GetDB
from app.node import node_manager
from app.node.user import core_users
from app.utils.logger import get_logger
from config import JOB_PERIODIC_USER_SYNC_INTERVAL

logger = get_logger("periodic-user-sync")


async def periodic_user_sync_job():
    """
    Periodic full sync of all active users to all nodes to ensure consistency.
    
    This job:
    1. Fetches all active users from the database
    2. Syncs them to each node individually (for better error tracking)
    3. Logs any nodes that are skipped or fail
    4. Provides a safety net for missed updates
    """
    logger.info("Starting periodic full user sync to all nodes")
    
    async with GetDB() as db:
        try:
            # Get all active users (only includes active and on_hold status)
            users = await core_users(db=db)
            nodes = await node_manager.get_nodes()
            
            if not nodes:
                logger.warning("No nodes available for periodic sync")
                return
            
            if not users:
                logger.info("No active users to sync")
                return
            
            logger.info(f"Syncing {len(users)} users to {len(nodes)} nodes")
            
            synced_nodes = []
            failed_nodes = []
            unhealthy_nodes = []
            
            # Sync to each node individually for better error tracking
            for node_id, node in nodes.items():
                try:
                    # Check node health before attempting sync
                    health = await node.get_health()
                    if health != Health.HEALTHY:
                        unhealthy_nodes.append((node_id, health.name))
                        logger.debug(
                            f"Skipping node {node_id} in periodic sync - health: {health.name}"
                        )
                        continue
                    
                    # Sync all users to this node
                    await node.update_users(users)
                    synced_nodes.append(node_id)
                    logger.debug(f"Successfully synced {len(users)} users to node {node_id}")
                    
                except Exception as e:
                    failed_nodes.append((node_id, str(e)))
                    logger.error(
                        f"Failed to sync users to node {node_id} during periodic sync: {e}"
                    )
            
            # Log summary
            total_nodes = len(nodes)
            logger.info(
                f"Periodic sync completed - {len(synced_nodes)}/{total_nodes} nodes synced successfully. "
                f"Unhealthy: {len(unhealthy_nodes)}, Failed: {len(failed_nodes)}"
            )
            
            if unhealthy_nodes:
                logger.warning(f"Unhealthy nodes skipped: {unhealthy_nodes}")
            
            if failed_nodes:
                logger.error(f"Failed nodes: {failed_nodes}")
            
        except Exception as e:
            logger.error(f"Error in periodic user sync job: {e}", exc_info=True)


# Schedule the periodic sync job
# Default: every 5 minutes (300 seconds)
# This can be configured via JOB_PERIODIC_USER_SYNC_INTERVAL in config.py
scheduler.add_job(
    periodic_user_sync_job,
    "interval",
    seconds=JOB_PERIODIC_USER_SYNC_INTERVAL,
    coalesce=True,
    max_instances=1,
    id="periodic_user_sync"
)

logger.info(f"Periodic user sync job scheduled to run every {JOB_PERIODIC_USER_SYNC_INTERVAL} seconds")

