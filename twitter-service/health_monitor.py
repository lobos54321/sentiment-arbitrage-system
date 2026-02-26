"""
Twitter Service Health Monitor

Monitors Twikit service health and triggers alerts/failover when issues detected.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILING = "failing"
    FAILED = "failed"


class HealthMetrics:
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.consecutive_failures = 0
        self.last_success_time = None
        self.last_failure_time = None
        self.failure_reasons: List[str] = []
        self.average_response_time = 0.0
        self.response_times: List[float] = []

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests

    @property
    def failure_rate(self) -> float:
        return 1.0 - self.success_rate


class TwitterHealthMonitor:
    """
    Monitors Twitter service health and determines when to failover to Grok.

    Failure Detection Criteria:
    1. Account suspended/banned (immediate failover)
    2. Rate limit exceeded repeatedly (degraded → failover)
    3. Authentication failures (re-login → failover)
    4. Consecutive failures > threshold
    5. Success rate < threshold over time window
    """

    def __init__(self):
        self.metrics = HealthMetrics()
        self.status = HealthStatus.HEALTHY

        # Thresholds
        self.max_consecutive_failures = 5
        self.min_success_rate = 0.7  # 70%
        self.degraded_threshold = 0.85  # 85%
        self.time_window_minutes = 15

        # Alert callbacks
        self.alert_callbacks = []

        # Recent events (for time-window analysis)
        self.recent_events = []

    def record_success(self, response_time: float):
        """Record successful request"""
        self.metrics.total_requests += 1
        self.metrics.successful_requests += 1
        self.metrics.consecutive_failures = 0
        self.metrics.last_success_time = datetime.now()
        self.metrics.response_times.append(response_time)

        # Keep only recent response times
        if len(self.metrics.response_times) > 100:
            self.metrics.response_times.pop(0)

        self.metrics.average_response_time = sum(self.metrics.response_times) / len(self.metrics.response_times)

        # Record event
        self._record_event('success', response_time)

        # Update status
        self._update_status()

    def record_failure(self, error_type: str, error_message: str):
        """Record failed request"""
        self.metrics.total_requests += 1
        self.metrics.failed_requests += 1
        self.metrics.consecutive_failures += 1
        self.metrics.last_failure_time = datetime.now()
        self.metrics.failure_reasons.append(f"{error_type}: {error_message}")

        # Keep only recent failures
        if len(self.metrics.failure_reasons) > 50:
            self.metrics.failure_reasons.pop(0)

        # Record event
        self._record_event('failure', 0, error_type, error_message)

        # Check for critical failures
        if self._is_critical_failure(error_type, error_message):
            logger.critical(f"🚨 CRITICAL FAILURE: {error_type} - {error_message}")
            self.status = HealthStatus.FAILED
            self._trigger_alert("critical_failure", error_type, error_message)

        # Update status
        self._update_status()

    def _record_event(self, event_type: str, response_time: float = 0,
                     error_type: str = None, error_message: str = None):
        """Record event for time-window analysis"""
        event = {
            'timestamp': datetime.now(),
            'type': event_type,
            'response_time': response_time,
            'error_type': error_type,
            'error_message': error_message
        }
        self.recent_events.append(event)

        # Clean old events
        cutoff = datetime.now() - timedelta(minutes=self.time_window_minutes)
        self.recent_events = [e for e in self.recent_events if e['timestamp'] > cutoff]

    def _is_critical_failure(self, error_type: str, error_message: str) -> bool:
        """Detect critical failures that require immediate failover"""
        critical_keywords = [
            'suspended',
            'banned',
            'locked',
            'account not found',
            'authorization failed',
            'invalid credentials',
            'forbidden',
            '403'
        ]

        error_lower = f"{error_type} {error_message}".lower()
        return any(keyword in error_lower for keyword in critical_keywords)

    def _update_status(self):
        """Update health status based on metrics"""
        # Calculate success rate in time window
        if len(self.recent_events) > 0:
            successes = sum(1 for e in self.recent_events if e['type'] == 'success')
            window_success_rate = successes / len(self.recent_events)
        else:
            window_success_rate = 1.0

        # Determine status
        previous_status = self.status

        if self.status == HealthStatus.FAILED:
            # Stay failed until manual recovery
            return

        if self.metrics.consecutive_failures >= self.max_consecutive_failures:
            self.status = HealthStatus.FAILING
        elif window_success_rate < self.min_success_rate:
            self.status = HealthStatus.FAILING
        elif window_success_rate < self.degraded_threshold:
            self.status = HealthStatus.DEGRADED
        else:
            self.status = HealthStatus.HEALTHY

        # Trigger alert on status change
        if self.status != previous_status:
            self._trigger_alert("status_change", previous_status, self.status)

    def _trigger_alert(self, alert_type: str, *args):
        """Trigger alert callbacks"""
        for callback in self.alert_callbacks:
            try:
                callback(alert_type, *args)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")

    def register_alert_callback(self, callback):
        """Register callback for alerts"""
        self.alert_callbacks.append(callback)

    def should_failover(self) -> bool:
        """Determine if should failover to Grok"""
        return self.status in [HealthStatus.FAILING, HealthStatus.FAILED]

    def get_health_report(self) -> Dict:
        """Get comprehensive health report"""
        return {
            'status': self.status.value,
            'should_failover': self.should_failover(),
            'metrics': {
                'total_requests': self.metrics.total_requests,
                'success_rate': round(self.metrics.success_rate * 100, 2),
                'consecutive_failures': self.metrics.consecutive_failures,
                'average_response_time_ms': round(self.metrics.average_response_time * 1000, 2),
                'last_success': self.metrics.last_success_time.isoformat() if self.metrics.last_success_time else None,
                'last_failure': self.metrics.last_failure_time.isoformat() if self.metrics.last_failure_time else None,
            },
            'recent_failures': self.metrics.failure_reasons[-10:],
            'recommendation': self._get_recommendation()
        }

    def _get_recommendation(self) -> str:
        """Get operational recommendation"""
        if self.status == HealthStatus.FAILED:
            return "⛔ CRITICAL: Immediate failover to Grok required. Account may be suspended."
        elif self.status == HealthStatus.FAILING:
            return "🚨 WARNING: High failure rate. Consider failover to Grok or account rotation."
        elif self.status == HealthStatus.DEGRADED:
            return "⚠️ DEGRADED: Monitor closely. Prepare for potential failover."
        else:
            return "✅ HEALTHY: Service operating normally."

    def reset(self):
        """Reset metrics (use after account switch)"""
        self.metrics = HealthMetrics()
        self.status = HealthStatus.HEALTHY
        self.recent_events = []
        logger.info("Health monitor reset")


# Global monitor instance
health_monitor = TwitterHealthMonitor()
