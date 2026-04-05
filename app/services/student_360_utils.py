"""
Student 360 Module Enhancements
Utilities for better logging, caching, and monitoring of Student 360 profiles
"""

import time
import logging
from typing import Optional
from functools import wraps
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Simple in-memory cache for Student 360 profiles
_profile_cache: dict[str, tuple] = {}
CACHE_TTL_SECONDS = 300  # 5 minutes

class Student360Cache:
    """Simple cache for Student 360 profiles"""
    
    @staticmethod
    def get(roll_no: str) -> Optional[dict]:
        """Get cached profile if not expired"""
        if roll_no in _profile_cache:
            data, timestamp = _profile_cache[roll_no]
            if datetime.now() - timestamp < timedelta(seconds=CACHE_TTL_SECONDS):
                logger.info(f"Cache HIT for Student 360: {roll_no}")
                return data
            else:
                del _profile_cache[roll_no]
        return None
    
    @staticmethod
    def set(roll_no: str, data: dict):
        """Cache profile data"""
        _profile_cache[roll_no] = (data, datetime.now())
        logger.info(f"Cached Student 360 profile: {roll_no}")
    
    @staticmethod
    def invalidate(roll_no: str):
        """Remove from cache"""
        if roll_no in _profile_cache:
            del _profile_cache[roll_no]
            logger.info(f"Invalidated cache for Student 360: {roll_no}")
    
    @staticmethod
    def clear_all():
        """Clear entire cache"""
        _profile_cache.clear()
        logger.info("Cleared all Student 360 profiles from cache")


def track_student_360_request(func):
    """Decorator to track Student 360 requests with timing and logging"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        roll_no = kwargs.get('roll_no', 'unknown')
        current_user = kwargs.get('current_user')
        user_email = getattr(current_user, 'email', 'unknown') if current_user else 'anonymous'
        
        start_time = time.time()
        logger.info(f"[Student 360] Request started: {roll_no} by {user_email}")
        
        try:
            result = await func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.info(f"[Student 360] Request completed: {roll_no} in {elapsed:.2f}s by {user_email}")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"[Student 360] Request failed: {roll_no} after {elapsed:.2f}s - {str(e)}")
            raise
    
    return wrapper


class Student360RequestMetrics:
    """Track metrics for Student 360 requests"""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_time = 0.0
        self.cache_hits = 0
        self.cache_misses = 0
    
    def record_request(self, success: bool, elapsed_time: float, cache_hit: bool = False):
        """Record a request metric"""
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        
        self.total_time += elapsed_time
        
        if cache_hit:
            self.cache_hits += 1
        else:
            self.cache_misses += 1
    
    def get_stats(self) -> dict:
        """Get current statistics"""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0,
            "avg_response_time": (self.total_time / self.total_requests) if self.total_requests > 0 else 0,
            "cache_hit_rate": (self.cache_hits / self.total_requests * 100) if self.total_requests > 0 else 0,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
        }
    
    def log_stats(self):
        """Log current statistics"""
        stats = self.get_stats()
        logger.info(f"""
        [Student 360 Metrics]
        - Total Requests: {stats['total_requests']}
        - Successful: {stats['successful_requests']}
        - Failed: {stats['failed_requests']}
        - Success Rate: {stats['success_rate']:.1f}%
        - Avg Response Time: {stats['avg_response_time']:.3f}s
        - Cache Hit Rate: {stats['cache_hit_rate']:.1f}%
        """)


# Global metrics instance
_metrics = Student360RequestMetrics()

def get_student_360_metrics() -> dict:
    """Get Student 360 module metrics"""
    return _metrics.get_stats()
