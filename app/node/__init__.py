import asyncio

from aiorwlock import RWLock
from PasarGuardNodeBridge import Health, NodeType, PasarGuardNode, create_node

from app.db.models import Node, NodeConnectionType, User
from app.models.user import UserResponse
from app.node.user import core_users, serialize_user_for_node, serialize_users_for_node
from app.utils.logger import get_logger

type_map = {
    NodeConnectionType.rest: NodeType.rest,
    NodeConnectionType.grpc: NodeType.grpc,
}


class NodeManager:
    def __init__(self):
        self._nodes: dict[int, PasarGuardNode] = {}
        self._lock = RWLock(fast=True)
        self.logger = get_logger("node-manager")

    async def update_node(self, node: Node) -> PasarGuardNode:
        async with self._lock.writer_lock:
            old_node: PasarGuardNode | None = self._nodes.get(node.id, None)
            if old_node is not None:
                try:
                    await old_node.set_health(Health.INVALID)
                    await old_node.stop()
                except Exception:
                    pass
                finally:
                    self._nodes.pop(node.id, None)

            new_node = create_node(
                connection=type_map[node.connection_type],
                address=node.address,
                port=node.port,
                server_ca=node.server_ca,
                api_key=node.api_key,
                name=node.name,
                logger=self.logger,
                extra={"id": node.id, "usage_coefficient": node.usage_coefficient},
            )

            self._nodes[node.id] = new_node

            return new_node

    async def remove_node(self, id: int) -> None:
        async with self._lock.writer_lock:
            old_node: PasarGuardNode | None = self._nodes.get(id, None)
            if old_node is not None:
                try:
                    await old_node.set_health(Health.INVALID)
                    await old_node.stop()
                except Exception:
                    pass
                finally:
                    self._nodes.pop(id, None)

    async def get_node(self, id: int) -> PasarGuardNode | None:
        async with self._lock.reader_lock:
            return self._nodes.get(id, None)

    async def get_nodes(self) -> dict[int, PasarGuardNode]:
        async with self._lock.reader_lock:
            return self._nodes

    async def get_healthy_nodes(self) -> list[tuple[int, PasarGuardNode]]:
        async with self._lock.reader_lock:
            nodes: list[tuple[int, PasarGuardNode]] = [
                (id, node) for id, node in self._nodes.items() if (await node.get_health() == Health.HEALTHY)
            ]
            return nodes

    async def get_broken_nodes(self) -> list[tuple[int, PasarGuardNode]]:
        async with self._lock.reader_lock:
            nodes: list[tuple[int, PasarGuardNode]] = [
                (id, node) for id, node in self._nodes.items() if (await node.get_health() == Health.BROKEN)
            ]
            return nodes

    async def get_not_connected_nodes(self) -> list[tuple[int, PasarGuardNode]]:
        async with self._lock.reader_lock:
            nodes: list[tuple[int, PasarGuardNode]] = [
                (id, node) for id, node in self._nodes.items() if (await node.get_health() == Health.NOT_CONNECTED)
            ]
            return nodes

    async def _update_users(self, users: list):
        synced_count = 0
        failed_nodes = []
        skipped_nodes = []
        
        async with self._lock.reader_lock:
            total_nodes = len(self._nodes)
            
            for node_id, node in self._nodes.items():
                try:
                    # Check node health before syncing
                    health = await node.get_health()
                    if health != Health.HEALTHY:
                        skipped_nodes.append((node_id, health.name))
                        self.logger.debug(
                            f"[Node {node_id}] Skipping bulk user sync - health status: {health.name}"
                        )
                        continue
                    
                    await node.update_users(users)
                    synced_count += 1
                    
                except Exception as e:
                    failed_nodes.append((node_id, str(e)))
                    self.logger.error(
                        f"[Node {node_id}] Failed to sync {len(users)} users: {e}"
                    )
        
        # Log summary
        if failed_nodes or skipped_nodes:
            self.logger.warning(
                f"Bulk user sync incomplete - Synced {len(users)} users to {synced_count}/{total_nodes} nodes. "
                f"Skipped: {len(skipped_nodes)} (unhealthy), Failed: {len(failed_nodes)} (errors)"
            )
            if failed_nodes:
                self.logger.error(f"Failed nodes: {failed_nodes}")
        else:
            self.logger.info(f"Successfully synced {len(users)} users to all {synced_count} nodes")

    async def update_users(self, users: list[User]):
        proto_users = await serialize_users_for_node(users)
        asyncio.create_task(self._update_users(proto_users))

    async def _update_user(self, user):
        synced_count = 0
        failed_nodes = []
        skipped_nodes = []
        
        async with self._lock.reader_lock:
            total_nodes = len(self._nodes)
            
            for node_id, node in self._nodes.items():
                try:
                    # Check node health before syncing
                    health = await node.get_health()
                    if health != Health.HEALTHY:
                        skipped_nodes.append((node_id, health.name))
                        self.logger.debug(
                            f"[Node {node_id}] Skipping user sync - health status: {health.name}"
                        )
                        continue
                    
                    await node.update_user(user)
                    synced_count += 1
                    
                except Exception as e:
                    failed_nodes.append((node_id, str(e)))
                    self.logger.error(
                        f"[Node {node_id}] Failed to sync user: {e}"
                    )
        
        # Log summary if there were any issues
        if failed_nodes or skipped_nodes:
            self.logger.warning(
                f"User sync incomplete - Synced to {synced_count}/{total_nodes} nodes. "
                f"Skipped: {len(skipped_nodes)} (unhealthy), Failed: {len(failed_nodes)} (errors)"
            )
            if failed_nodes:
                self.logger.error(f"Failed nodes: {failed_nodes}")
        elif synced_count > 0:
            self.logger.debug(f"Successfully synced user to all {synced_count} nodes")

    async def update_user(self, user: UserResponse, inbounds: list[str] = None):
        proto_user = serialize_user_for_node(user.id, user.username, user.proxy_settings.dict(), inbounds)
        await self._update_user(proto_user)

    async def remove_user(self, user: UserResponse):
        proto_user = serialize_user_for_node(user.id, user.username, user.proxy_settings.dict())
        await self._update_user(proto_user)


node_manager: NodeManager = NodeManager()


__all__ = ["core_users", "node_manager"]
