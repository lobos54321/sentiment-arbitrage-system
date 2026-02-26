"""
Twitter Account Pool Manager

Manages multiple Twitter accounts with automatic rotation to prevent bans.
"""

import asyncio
import os
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json
import logging

from twikit import Client
from health_monitor import health_monitor, HealthStatus

logger = logging.getLogger(__name__)


class TwitterAccount:
    """Single Twitter account with status tracking"""

    def __init__(self, username: str, email: str, password: str):
        self.username = username
        self.email = email
        self.password = password
        self.client: Optional[Client] = None
        self.cookies_path = f"./cache/cookies_{username}.json"
        self.is_active = False
        self.is_banned = False
        self.last_used = None
        self.total_requests = 0
        self.failed_requests = 0
        self.created_at = datetime.now()

    async def login(self) -> bool:
        """Login to Twitter"""
        try:
            self.client = Client('en-US')

            # Try loading cookies first
            if os.path.exists(self.cookies_path):
                try:
                    self.client.load_cookies(self.cookies_path)
                    logger.info(f"✅ Loaded cookies for @{self.username}")
                    self.is_active = True
                    return True
                except Exception as e:
                    logger.warning(f"Failed to load cookies for @{self.username}: {e}")

            # Fresh login
            await self.client.login(
                auth_info_1=self.username,
                auth_info_2=self.email,
                password=self.password
            )

            # Save cookies
            self.client.save_cookies(self.cookies_path)
            logger.info(f"✅ Logged in @{self.username} and saved cookies")

            self.is_active = True
            return True

        except Exception as e:
            logger.error(f"❌ Login failed for @{self.username}: {e}")
            self.is_banned = self._check_if_banned(str(e))
            return False

    def _check_if_banned(self, error_message: str) -> bool:
        """Check if error indicates account ban"""
        ban_keywords = ['suspended', 'banned', 'locked', 'forbidden', 'authorization']
        return any(keyword in error_message.lower() for keyword in ban_keywords)

    def mark_used(self):
        """Mark account as used"""
        self.last_used = datetime.now()
        self.total_requests += 1

    def mark_failed(self):
        """Mark request as failed"""
        self.failed_requests += 1

    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.total_requests == 0:
            return 1.0
        return 1.0 - (self.failed_requests / self.total_requests)

    @property
    def health_score(self) -> float:
        """Calculate health score (0-1)"""
        if self.is_banned:
            return 0.0

        # Factors: success rate, recency, total usage
        success_score = self.success_rate

        # Prefer less-used accounts
        if self.total_requests == 0:
            usage_score = 1.0
        else:
            usage_score = max(0, 1.0 - (self.total_requests / 1000))

        # Prefer recently successful accounts
        if self.last_used:
            hours_since_use = (datetime.now() - self.last_used).total_seconds() / 3600
            recency_score = min(1.0, hours_since_use / 24)  # Prefer accounts rested for a while
        else:
            recency_score = 1.0

        return (success_score * 0.5 + usage_score * 0.3 + recency_score * 0.2)


class AccountPool:
    """
    Manages pool of Twitter accounts with intelligent rotation.

    Features:
    - Automatic account rotation to distribute load
    - Health-based selection (prefer healthy accounts)
    - Automatic failover when account fails
    - Rest period for accounts to avoid rate limits
    """

    def __init__(self):
        self.accounts: List[TwitterAccount] = []
        self.current_account: Optional[TwitterAccount] = None
        self.min_rest_minutes = 30  # Minimum rest between heavy usage

    def add_account(self, username: str, email: str, password: str):
        """Add account to pool"""
        account = TwitterAccount(username, email, password)
        self.accounts.append(account)
        logger.info(f"Added account @{username} to pool (total: {len(self.accounts)})")

    async def initialize(self):
        """Initialize all accounts"""
        logger.info(f"Initializing {len(self.accounts)} accounts...")

        for account in self.accounts:
            success = await account.login()
            if success:
                logger.info(f"✅ @{account.username} ready")
            else:
                logger.warning(f"⚠️ @{account.username} failed to initialize")

        # Select initial account
        self.current_account = self._select_best_account()
        if self.current_account:
            logger.info(f"🎯 Using @{self.current_account.username} as primary account")

    def _select_best_account(self) -> Optional[TwitterAccount]:
        """Select best available account based on health score"""
        available = [acc for acc in self.accounts if acc.is_active and not acc.is_banned]

        if not available:
            logger.error("❌ No available accounts!")
            return None

        # Sort by health score
        available.sort(key=lambda a: a.health_score, reverse=True)

        best = available[0]
        logger.info(f"Selected @{best.username} (health: {best.health_score:.2f}, requests: {best.total_requests})")

        return best

    async def rotate_account(self, reason: str = "manual") -> bool:
        """Rotate to next best account"""
        logger.warning(f"🔄 Rotating account (reason: {reason})")

        # Mark current account issues if auto-rotation
        if self.current_account and reason != "manual":
            self.current_account.is_banned = True

        # Select new account
        new_account = self._select_best_account()

        if not new_account:
            logger.error("❌ No available accounts for rotation!")
            return False

        if new_account == self.current_account:
            logger.warning("⚠️ No better account available")
            return False

        # Switch
        old_username = self.current_account.username if self.current_account else "none"
        self.current_account = new_account

        logger.info(f"✅ Rotated from @{old_username} to @{new_account.username}")

        # Reset health monitor for new account
        health_monitor.reset()

        return True

    def get_current_client(self) -> Optional[Client]:
        """Get current active Twikit client"""
        if not self.current_account:
            logger.error("No current account!")
            return None

        self.current_account.mark_used()
        return self.current_account.client

    def mark_current_failed(self):
        """Mark current account request as failed"""
        if self.current_account:
            self.current_account.mark_failed()

    def get_pool_status(self) -> Dict:
        """Get pool status"""
        return {
            'total_accounts': len(self.accounts),
            'active_accounts': sum(1 for a in self.accounts if a.is_active and not a.is_banned),
            'banned_accounts': sum(1 for a in self.accounts if a.is_banned),
            'current_account': self.current_account.username if self.current_account else None,
            'accounts': [
                {
                    'username': acc.username,
                    'active': acc.is_active,
                    'banned': acc.is_banned,
                    'health_score': round(acc.health_score, 2),
                    'total_requests': acc.total_requests,
                    'success_rate': round(acc.success_rate * 100, 2)
                }
                for acc in self.accounts
            ]
        }


# Global pool instance
account_pool = AccountPool()
