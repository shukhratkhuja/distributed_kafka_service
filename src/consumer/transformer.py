"""
Event Transformer - Transforms raw events with validation, enrichment, and deduplication
"""

import json
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
import ipaddress
from user_agents import parse as parse_user_agent
import hashlib
import re

logger = logging.getLogger(__name__)

@dataclass
class TransformedEvent:
    """Structured representation of a transformed event"""
    event_id: str
    user_id: int
    event_type: str
    event_date: str
    timestamp: datetime
    hour_of_day: int
    day_of_week: int
    is_weekend: bool
    event_category: str
    session_id: str
    ip_address: str
    user_agent: str
    referrer: Optional[str]
    page_url: str
    properties: Dict[str, Any]
    
    # Enriched fields
    browser: Optional[str] = None
    os: Optional[str] = None
    device_type: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    
    processed_at: Optional[datetime] = None

class EventDeduplicator:
    """Handles event deduplication using LRU cache"""
    
    def __init__(self, max_size: int = 100000):
        self.max_size = max_size
        self.seen_events = {}  # event_id -> timestamp
        self.access_order = []  # For LRU eviction
        
    def is_duplicate(self, event_id: str) -> bool:
        """Check if event is duplicate and track it"""
        if event_id in self.seen_events:
            # Move to end for LRU
            self.access_order.remove(event_id)
            self.access_order.append(event_id)
            return True
            
        # Add new event
        self.seen_events[event_id] = datetime.utcnow()
        self.access_order.append(event_id)
        
        # Cleanup if needed
        if len(self.seen_events) > self.max_size:
            self._cleanup()
            
        return False
    
    def _cleanup(self):
        """Remove oldest entries to maintain size limit"""
        remove_count = len(self.seen_events) - int(self.max_size * 0.8)  # Remove 20%
        
        for _ in range(remove_count):
            if self.access_order:
                oldest = self.access_order.pop(0)
                self.seen_events.pop(oldest, None)

class EventTransformer:
    """Main event transformer with validation, enrichment, and deduplication"""
    
    def __init__(self):
        self.deduplicator = EventDeduplicator()
        self.stats = {
            'processed': 0,
            'duplicates': 0,
            'errors': 0,
            'enriched': 0,
            'validation_errors': 0
        }
        
        # Event category mapping
        self.event_categories = {
            # Engagement events
            'page_view': 'engagement',
            'button_click': 'engagement', 
            'scroll': 'engagement',
            'hover': 'engagement',
            'focus': 'engagement',
            'blur': 'engagement',
            'resize': 'engagement',
            'search': 'engagement',
            'filter': 'engagement',
            'sort': 'engagement',
            'video_play': 'engagement',
            'video_pause': 'engagement',
            'video_complete': 'engagement',
            'audio_play': 'engagement',
            'download': 'engagement',
            'print': 'engagement',
            'bookmark': 'engagement',
            'copy': 'engagement',
            'select_text': 'engagement',
            
            # Conversion events
            'signup': 'conversion',
            'form_submit': 'conversion',
            'newsletter_signup': 'conversion',
            'trial_start': 'conversion',
            'demo_request': 'conversion',
            'contact_submit': 'conversion',
            'quote_request': 'conversion',
            'callback_request': 'conversion',
            'whitepaper_download': 'conversion',
            'webinar_registration': 'conversion',
            
            # Revenue events
            'purchase': 'revenue',
            'subscription_start': 'revenue',
            'subscription_renew': 'revenue',
            'subscription_upgrade': 'revenue',
            'subscription_downgrade': 'revenue',
            'refund_request': 'revenue',
            'payment_method_add': 'revenue',
            'billing_update': 'revenue',
            'invoice_paid': 'revenue',
            'plan_change': 'revenue',
            
            # E-commerce events
            'cart_add': 'ecommerce',
            'cart_remove': 'ecommerce',
            'cart_view': 'ecommerce',
            'cart_clear': 'ecommerce',
            'wishlist_add': 'ecommerce',
            'wishlist_remove': 'ecommerce',
            'product_view': 'ecommerce',
            'product_compare': 'ecommerce',
            'checkout_start': 'ecommerce',
            'checkout_complete': 'ecommerce',
            'coupon_apply': 'ecommerce',
            'coupon_remove': 'ecommerce',
            'shipping_select': 'ecommerce',
            'payment_select': 'ecommerce',
            
            # Social events
            'share': 'social',
            'like': 'social',
            'unlike': 'social',
            'comment': 'social',
            'reply': 'social',
            'follow': 'social',
            'unfollow': 'social',
            'mention': 'social',
            'tag': 'social',
            'rate': 'social',
            'review': 'social',
            'recommend': 'social',
            'report': 'social',
            'block': 'social',
            
            # Authentication events
            'login': 'authentication',
            'logout': 'authentication',
            'password_reset': 'authentication',
            'password_change': 'authentication',
            'email_verify': 'authentication',
            'phone_verify': 'authentication',
            'two_factor_enable': 'authentication',
            'two_factor_disable': 'authentication',
            'account_lock': 'authentication',
            'account_unlock': 'authentication',
            'profile_update': 'authentication',
            'privacy_update': 'authentication',
            
            # Technical events
            'error': 'technical',
            'exception': 'technical',
            'timeout': 'technical',
            'api_call': 'technical',
            'database_query': 'technical',
            'cache_hit': 'technical',
            'cache_miss': 'technical',
            'file_upload': 'technical',
            'file_process': 'technical',
            'batch_process': 'technical',
            'system_alert': 'technical',
            'performance_issue': 'technical',
            'security_event': 'technical'
        }
        
        # Required fields for validation
        self.required_fields = {
            'event_id': str,
            'user_id': (int, str),  # Can be int or string
            'event_type': str,
            'timestamp': str
        }
        
        # Optional but common fields
        self.optional_fields = {
            'session_id': str,
            'ip_address': str,
            'user_agent': str,
            'referrer': (str, type(None)),
            'page_url': str,
            'properties': dict
        }
    
    def _validate_event(self, event: Dict[str, Any]) -> bool:
        """Validate event structure and required fields"""
        try:
            # Check required fields
            for field, expected_type in self.required_fields.items():
                if field not in event:
                    logger.warning(f"Missing required field: {field}")
                    return False
                    
                if not isinstance(event[field], expected_type):
                    # Special handling for user_id - can be string that converts to int
                    if field == 'user_id' and isinstance(event[field], str):
                        try:
                            int(event[field])
                            continue
                        except ValueError:
                            pass
                    logger.warning(f"Invalid type for {field}: expected {expected_type}, got {type(event[field])}")
                    return False
            
            # Validate event_id format (should be non-empty string)
            if not event['event_id'].strip():
                logger.warning("Empty event_id")
                return False
                
            # Validate timestamp format
            if not self._is_valid_timestamp(event['timestamp']):
                logger.warning(f"Invalid timestamp format: {event['timestamp']}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False
    
    def _is_valid_timestamp(self, timestamp_str: str) -> bool:
        """Check if timestamp string is in valid ISO format"""
        try:
            datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return True
        except (ValueError, TypeError):
            # Try other common formats
            formats = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S.%f'
            ]
            
            for fmt in formats:
                try:
                    datetime.strptime(timestamp_str, fmt)
                    return True
                except ValueError:
                    continue
                    
            return False
    
    def _normalize_timestamp(self, timestamp_str: str) -> datetime:
        """Convert timestamp string to datetime object"""
        try:
            # Try ISO format first
            if 'Z' in timestamp_str:
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                return datetime.fromisoformat(timestamp_str)
        except ValueError:
            # Try other formats
            formats = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S.%f'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(timestamp_str, fmt)
                except ValueError:
                    continue
                    
            # If all else fails, use current time
            logger.warning(f"Could not parse timestamp {timestamp_str}, using current time")
            return datetime.utcnow()
    
    def _derive_time_fields(self, timestamp: datetime) -> Dict[str, Any]:
        """Derive time-based fields from timestamp"""
        return {
            'event_date': timestamp.date().isoformat(),
            'hour_of_day': timestamp.hour,
            'day_of_week': timestamp.weekday(),  # Monday = 0
            'is_weekend': timestamp.weekday() >= 5  # Saturday = 5, Sunday = 6
        }
    
    def _get_event_category(self, event_type: str) -> str:
        """Map event type to category"""
        return self.event_categories.get(event_type.lower(), 'other')
    
    def _enrich_user_agent(self, user_agent_string: str) -> Dict[str, Optional[str]]:
        """Parse user agent string for device information"""
        try:
            if not user_agent_string:
                return {'browser': None, 'os': None, 'device_type': None}
                
            parsed = parse_user_agent(user_agent_string)
            
            device_type = 'desktop'
            if parsed.is_mobile:
                device_type = 'mobile'
            elif parsed.is_tablet:
                device_type = 'tablet'
            elif parsed.is_bot:
                device_type = 'bot'
                
            return {
                'browser': parsed.browser.family if parsed.browser else None,
                'os': parsed.os.family if parsed.os else None,
                'device_type': device_type
            }
        except Exception as e:
            logger.warning(f"Failed to parse user agent: {e}")
            return {'browser': None, 'os': None, 'device_type': None}
    
    def _enrich_ip_address(self, ip_string: str) -> Dict[str, Optional[str]]:
        """Enrich IP address with geographical information"""
        try:
            if not ip_string:
                return {'country': None, 'region': None, 'city': None}
                
            # Validate IP address
            ip = ipaddress.ip_address(ip_string)
            
            # For demo purposes, return mock geo data
            # In production, you'd use a real geolocation service like MaxMind
            if ip.is_private:
                return {'country': 'Private', 'region': 'Private', 'city': 'Private'}
            
            # Mock geo data based on IP hash for consistency
            ip_hash = int(hashlib.md5(ip_string.encode()).hexdigest()[:8], 16)
            
            countries = ['US', 'UK', 'DE', 'FR', 'JP', 'AU', 'CA', 'BR', 'IN', 'CN']
            regions = ['CA', 'NY', 'TX', 'FL', 'WA', 'London', 'Berlin', 'Tokyo']
            cities = ['San Francisco', 'New York', 'Los Angeles', 'Chicago', 'Seattle']
            
            return {
                'country': countries[ip_hash % len(countries)],
                'region': regions[ip_hash % len(regions)],
                'city': cities[ip_hash % len(cities)]
            }
            
        except Exception as e:
            logger.warning(f"Failed to enrich IP address {ip_string}: {e}")
            return {'country': None, 'region': None, 'city': None}
    
    def _sanitize_properties(self, properties: Any) -> Dict[str, Any]:
        """Sanitize and normalize properties field"""
        if properties is None:
            return {}
            
        if isinstance(properties, dict):
            return properties
            
        if isinstance(properties, str):
            try:
                return json.loads(properties)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON in properties field")
                return {'raw_properties': properties}
        
        # Convert other types to dict
        return {'value': str(properties)}
    
    def transform_event(self, raw_event: Dict[str, Any]) -> Optional[TransformedEvent]:
        """Transform a single raw event"""
        self.stats['processed'] += 1
        
        try:
            # Validate event
            if not self._validate_event(raw_event):
                self.stats['validation_errors'] += 1
                self.stats['errors'] += 1
                return None
            
            # Check for duplicates
            if self.deduplicator.is_duplicate(raw_event['event_id']):
                self.stats['duplicates'] += 1
                return None
            
            # Normalize user_id to int
            user_id = raw_event['user_id']
            if isinstance(user_id, str):
                try:
                    user_id = int(user_id)
                except ValueError:
                    logger.warning(f"Could not convert user_id to int: {user_id}")
                    self.stats['errors'] += 1
                    return None
            
            # Parse timestamp
            timestamp = self._normalize_timestamp(raw_event['timestamp'])
            time_fields = self._derive_time_fields(timestamp)
            
            # Get event category
            event_category = self._get_event_category(raw_event['event_type'])
            
            # Enrich user agent
            user_agent_info = self._enrich_user_agent(raw_event.get('user_agent', ''))
            
            # Enrich IP address
            geo_info = self._enrich_ip_address(raw_event.get('ip_address', ''))
            
            # Sanitize properties
            properties = self._sanitize_properties(raw_event.get('properties'))
            
            # Create transformed event
            transformed = TransformedEvent(
                event_id=raw_event['event_id'],
                user_id=user_id,
                event_type=raw_event['event_type'].lower(),
                event_date=time_fields['event_date'],
                timestamp=timestamp,
                hour_of_day=time_fields['hour_of_day'],
                day_of_week=time_fields['day_of_week'],
                is_weekend=time_fields['is_weekend'],
                event_category=event_category,
                session_id=raw_event.get('session_id', ''),
                ip_address=raw_event.get('ip_address', ''),
                user_agent=raw_event.get('user_agent', ''),
                referrer=raw_event.get('referrer'),
                page_url=raw_event.get('page_url', ''),
                properties=properties,
                browser=user_agent_info['browser'],
                os=user_agent_info['os'],
                device_type=user_agent_info['device_type'],
                country=geo_info['country'],
                region=geo_info['region'],
                city=geo_info['city'],
                processed_at=datetime.utcnow()
            )
            
            if user_agent_info['browser'] or geo_info['country']:
                self.stats['enriched'] += 1
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming event: {e}")
            self.stats['errors'] += 1
            return None
    
    def transform_batch(self, raw_events: List[Dict[str, Any]]) -> List[TransformedEvent]:
        """Transform a batch of raw events"""
        transformed_events = []
        
        for raw_event in raw_events:
            transformed = self.transform_event(raw_event)
            if transformed:
                transformed_events.append(transformed)
                
        return transformed_events
    
    def get_stats(self) -> Dict[str, int]:
        """Get transformation statistics"""
        return self.stats.copy()

# Utility functions for database-specific formatting
def event_to_clickhouse_dict(event: TransformedEvent) -> Dict[str, Any]:
    """Convert transformed event to ClickHouse format"""
    return {
        'event_id': event.event_id,
        'user_id': event.user_id,
        'event_type': event.event_type,
        'event_date': event.event_date,
        'timestamp': event.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        'hour_of_day': event.hour_of_day,
        'day_of_week': event.day_of_week,
        'is_weekend': 1 if event.is_weekend else 0,
        'event_category': event.event_category,
        'session_id': event.session_id,
        'ip_address': event.ip_address,
        'user_agent': event.user_agent,
        'referrer': event.referrer or '',
        'page_url': event.page_url,
        'properties': json.dumps(event.properties) if event.properties else '{}',
        'processed_at': event.processed_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if event.processed_at else ''
    }

def event_to_mongodb_dict(event: TransformedEvent) -> Dict[str, Any]:
    """Convert transformed event to MongoDB format"""
    doc = asdict(event)
    
    # Convert datetime objects to ISO strings for MongoDB
    doc['timestamp'] = event.timestamp.isoformat() + 'Z'
    if event.processed_at:
        doc['processed_at'] = event.processed_at.isoformat() + 'Z'
    
    # Add enrichment metadata
    doc['enrichment'] = {
        'geo_enriched': bool(event.country),
        'user_agent_parsed': bool(event.browser),
        'processed_version': '1.0'
    }
    
    return doc