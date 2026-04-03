"""Base plugin ABC for HITL integrations."""
from __future__ import annotations

from abc import ABC, abstractmethod

from hitl_middleware.models import ApprovalIntent, NotificationIntent


class HitlPlugin(ABC):
    """Abstract base class for HITL integration plugins.

    Plugins dispatch notifications and approval requests to external systems
    (webhooks, Slack, PagerDuty, email, etc.).
    """

    @abstractmethod
    async def send_notification(self, intent: NotificationIntent) -> bool:
        """Fire-and-forget notification for INFORM/WARN outcomes.

        Returns True if notification was sent successfully, False otherwise.
        Failures should be logged, not raised.
        """
        ...

    @abstractmethod
    async def send_approval_request(self, intent: ApprovalIntent) -> bool:
        """Send approval request with callback URLs for REQUIRE_APPROVAL outcomes.

        Returns True if at least one delivery channel succeeded.
        Failures should be logged, not raised.
        """
        ...

    async def close(self) -> None:
        """Cleanup resources. Override if the plugin holds connections."""
