/**
 * ClickHouse Advanced Analytics Platform - Client-Side Tracking
 * Comprehensive event tracking with data cleaning and journey analysis
 */

(function(window, document) {
    'use strict';

    // Configuration
    const config = {
        endpoint: window.analyticsConfig?.endpoint || '/api/events',
        batchSize: window.analyticsConfig?.batchSize || 10,
        flushInterval: window.analyticsConfig?.flushInterval || 5000,
        sessionTimeout: window.analyticsConfig?.sessionTimeout || 30 * 60 * 1000, // 30 minutes
        enableAutoTracking: window.analyticsConfig?.enableAutoTracking !== false,
        enableCrossDomainTracking: window.analyticsConfig?.enableCrossDomainTracking || false,
        cookieDomain: window.analyticsConfig?.cookieDomain || null,
        debug: window.analyticsConfig?.debug || false
    };

    // Utility functions
    const utils = {
        // Generate unique IDs
        generateId: function() {
            return 'xxxx-xxxx-4xxx-yxxx'.replace(/[xy]/g, function(c) {
                const r = Math.random() * 16 | 0;
                const v = c === 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        },

        // Get current timestamp in ISO format
        getTimestamp: function() {
            return new Date().toISOString();
        },

        // Cookie management
        setCookie: function(name, value, days) {
            const expires = days ? `; expires=${new Date(Date.now() + days * 864e5).toUTCString()}` : '';
            const domain = config.cookieDomain ? `; domain=${config.cookieDomain}` : '';
            document.cookie = `${name}=${value}${expires}; path=/${domain}; SameSite=Lax`;
        },

        getCookie: function(name) {
            const value = `; ${document.cookie}`;
            const parts = value.split(`; ${name}=`);
            if (parts.length === 2) return parts.pop().split(';').shift();
            return null;
        },

        // URL parameter extraction
        getUrlParams: function(url = window.location.href) {
            const params = {};
            const urlObj = new URL(url);
            
            // UTM parameters
            ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(param => {
                const value = urlObj.searchParams.get(param);
                if (value) params[param] = value;
            });

            // Google Ads parameters
            ['gclid', 'gbraid', 'wbraid'].forEach(param => {
                const value = urlObj.searchParams.get(param);
                if (value) params[param] = value;
            });

            return params;
        },

        // Device fingerprinting
        getDeviceFingerprint: function() {
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            ctx.textBaseline = 'top';
            ctx.font = '14px Arial';
            ctx.fillText('Device fingerprint', 2, 2);
            
            const fingerprint = [
                navigator.userAgent,
                navigator.language,
                screen.width + 'x' + screen.height,
                new Date().getTimezoneOffset(),
                canvas.toDataURL()
            ].join('|');

            return this.hashCode(fingerprint).toString();
        },

        hashCode: function(str) {
            let hash = 0;
            for (let i = 0; i < str.length; i++) {
                const char = str.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash = hash & hash; // Convert to 32-bit integer
            }
            return Math.abs(hash);
        },

        // Data validation and cleaning
        cleanString: function(str, maxLength = 255) {
            if (!str) return '';
            return str.toString().trim().substring(0, maxLength);
        },

        validateEmail: function(email) {
            const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            return re.test(email);
        },

        // Debug logging
        log: function(...args) {
            if (config.debug) {
                console.log('[Analytics]', ...args);
            }
        }
    };

    // Session management
    const sessionManager = {
        init: function() {
            this.updateSession();
            this.startSessionTimer();
        },

        updateSession: function() {
            const now = Date.now();
            const lastActivity = parseInt(utils.getCookie('_analytics_last_activity') || '0');
            
            if (!lastActivity || (now - lastActivity) > config.sessionTimeout) {
                // New session
                this.sessionId = utils.generateId();
                this.sessionStart = now;
                this.visitId = parseInt(utils.getCookie('_analytics_visit_id') || '0') + 1;
                this.pageViews = 0;
                
                utils.setCookie('_analytics_session_id', this.sessionId);
                utils.setCookie('_analytics_session_start', this.sessionStart.toString());
                utils.setCookie('_analytics_visit_id', this.visitId.toString(), 365);
                
                utils.log('New session started:', this.sessionId);
            } else {
                // Existing session
                this.sessionId = utils.getCookie('_analytics_session_id') || utils.generateId();
                this.sessionStart = parseInt(utils.getCookie('_analytics_session_start') || now.toString());
                this.visitId = parseInt(utils.getCookie('_analytics_visit_id') || '1');
                this.pageViews = parseInt(utils.getCookie('_analytics_page_views') || '0');
            }

            utils.setCookie('_analytics_last_activity', now.toString());
        },

        startSessionTimer: function() {
            // Update session activity every minute
            setInterval(() => {
                utils.setCookie('_analytics_last_activity', Date.now().toString());
            }, 60000);
        },

        incrementPageViews: function() {
            this.pageViews++;
            utils.setCookie('_analytics_page_views', this.pageViews.toString());
        },

        getSessionDuration: function() {
            return Math.floor((Date.now() - this.sessionStart) / 1000);
        }
    };

    // User identification
    const userManager = {
        init: function() {
            this.anonymousId = utils.getCookie('_analytics_anonymous_id') || utils.generateId();
            this.userId = utils.getCookie('_analytics_user_id') || '';
            this.deviceFingerprint = utils.getCookie('_analytics_device_fp') || utils.getDeviceFingerprint();
            
            utils.setCookie('_analytics_anonymous_id', this.anonymousId, 365);
            utils.setCookie('_analytics_device_fp', this.deviceFingerprint, 365);
        },

        identify: function(userId, traits = {}) {
            this.userId = userId;
            utils.setCookie('_analytics_user_id', userId, 365);
            
            // Send identification event
            analytics.track('user_identified', {
                previous_anonymous_id: this.anonymousId,
                traits: traits
            });

            // Call identify API
            fetch(config.endpoint.replace('/events', '/identify'), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    user_id: userId,
                    anonymous_id: this.anonymousId,
                    traits: traits
                })
            }).catch(error => {
                utils.log('Failed to send identification:', error);
            });

            utils.log('User identified:', userId);
        }
    };

    // Event queue and batching
    const eventQueue = {
        events: [],
        
        add: function(event) {
            // Data cleaning and validation
            const cleanedEvent = this.cleanEvent(event);
            
            if (this.validateEvent(cleanedEvent)) {
                this.events.push(cleanedEvent);
                utils.log('Event queued:', cleanedEvent);
                
                if (this.events.length >= config.batchSize) {
                    this.flush();
                }
            } else {
                utils.log('Invalid event rejected:', event);
            }
        },

        cleanEvent: function(event) {
            return {
                event_id: event.event_id || utils.generateId(),
                event_time: event.event_time || utils.getTimestamp(),
                event_type: utils.cleanString(event.event_type, 100),
                user_id: utils.cleanString(userManager.userId),
                anonymous_id: utils.cleanString(userManager.anonymousId),
                session_id: utils.cleanString(sessionManager.sessionId),
                visit_id: sessionManager.visitId,
                device_fingerprint: utils.cleanString(userManager.deviceFingerprint),
                user_agent: utils.cleanString(navigator.userAgent, 1000),
                ip_address: event.ip_address || '', // Will be set server-side
                page_url: utils.cleanString(event.page_url || window.location.href, 2000),
                page_title: utils.cleanString(event.page_title || document.title, 500),
                referrer_url: utils.cleanString(event.referrer_url || document.referrer, 2000),
                utm_source: utils.cleanString(event.utm_source || ''),
                utm_medium: utils.cleanString(event.utm_medium || ''),
                utm_campaign: utils.cleanString(event.utm_campaign || ''),
                utm_content: utils.cleanString(event.utm_content || ''),
                utm_term: utils.cleanString(event.utm_term || ''),
                gclid: utils.cleanString(event.gclid || ''),
                gbraid: utils.cleanString(event.gbraid || ''),
                wbraid: utils.cleanString(event.wbraid || ''),
                revenue: parseFloat(event.revenue || 0),
                currency: utils.cleanString(event.currency || 'USD', 3),
                order_id: utils.cleanString(event.order_id || ''),
                product_id: utils.cleanString(event.product_id || ''),
                product_category: utils.cleanString(event.product_category || ''),
                quantity: parseInt(event.quantity || 0),
                custom_properties: event.custom_properties || {},
                session_start_time: new Date(sessionManager.sessionStart).toISOString(),
                session_duration: sessionManager.getSessionDuration(),
                page_views_in_session: sessionManager.pageViews,
                is_bounce: sessionManager.pageViews <= 1 && sessionManager.getSessionDuration() < 30
            };
        },

        validateEvent: function(event) {
            return event.event_id && 
                   event.event_type && 
                   event.anonymous_id && 
                   event.session_id;
        },

        flush: function() {
            if (this.events.length === 0) return;

            const eventsToSend = [...this.events];
            this.events = [];

            fetch(config.endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    events: eventsToSend
                }),
                keepalive: true
            }).then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                utils.log(`Sent ${eventsToSend.length} events successfully`);
            }).catch(error => {
                utils.log('Failed to send events:', error);
                // Re-queue events for retry
                this.events.unshift(...eventsToSend);
            });
        }
    };

    // Auto-tracking functionality
    const autoTracker = {
        init: function() {
            if (!config.enableAutoTracking) return;

            this.trackPageView();
            this.setupClickTracking();
            this.setupFormTracking();
            this.setupScrollTracking();
            this.setupUnloadTracking();
        },

        trackPageView: function() {
            sessionManager.incrementPageViews();
            
            const urlParams = utils.getUrlParams();
            analytics.track('page_view', {
                ...urlParams,
                page_path: window.location.pathname,
                page_search: window.location.search,
                page_hash: window.location.hash
            });
        },

        setupClickTracking: function() {
            document.addEventListener('click', (event) => {
                const element = event.target;
                
                // Track link clicks
                if (element.tagName === 'A' || element.closest('a')) {
                    const link = element.tagName === 'A' ? element : element.closest('a');
                    analytics.track('link_click', {
                        link_url: link.href,
                        link_text: link.textContent.trim(),
                        link_domain: link.hostname,
                        is_external: link.hostname !== window.location.hostname
                    });
                }

                // Track button clicks
                if (element.tagName === 'BUTTON' || element.type === 'button' || element.type === 'submit') {
                    analytics.track('button_click', {
                        button_text: element.textContent.trim(),
                        button_type: element.type,
                        button_id: element.id,
                        button_class: element.className
                    });
                }
            });
        },

        setupFormTracking: function() {
            document.addEventListener('submit', (event) => {
                const form = event.target;
                if (form.tagName === 'FORM') {
                    analytics.track('form_submit', {
                        form_id: form.id,
                        form_name: form.name,
                        form_action: form.action,
                        form_method: form.method
                    });
                }
            });
        },

        setupScrollTracking: function() {
            let maxScroll = 0;
            let scrollTimer;

            window.addEventListener('scroll', () => {
                const scrollPercent = Math.round(
                    (window.scrollY / (document.body.scrollHeight - window.innerHeight)) * 100
                );
                
                if (scrollPercent > maxScroll) {
                    maxScroll = scrollPercent;
                }

                clearTimeout(scrollTimer);
                scrollTimer = setTimeout(() => {
                    if (maxScroll >= 25 && maxScroll % 25 === 0) {
                        analytics.track('scroll_depth', {
                            scroll_percent: maxScroll
                        });
                    }
                }, 500);
            });
        },

        setupUnloadTracking: function() {
            window.addEventListener('beforeunload', () => {
                eventQueue.flush();
            });

            // Page visibility API for better session tracking
            document.addEventListener('visibilitychange', () => {
                if (document.visibilityState === 'hidden') {
                    eventQueue.flush();
                }
            });
        }
    };

    // Main analytics object
    window.analytics = {
        // Initialize the tracker
        init: function() {
            userManager.init();
            sessionManager.init();
            autoTracker.init();
            
            // Set up periodic flush
            setInterval(() => {
                eventQueue.flush();
            }, config.flushInterval);

            utils.log('Analytics initialized');
        },

        // Track custom events
        track: function(eventType, properties = {}) {
            const urlParams = utils.getUrlParams();
            
            eventQueue.add({
                event_type: eventType,
                ...urlParams,
                ...properties
            });
        },

        // Identify users
        identify: function(userId, traits = {}) {
            userManager.identify(userId, traits);
        },

        // E-commerce tracking helpers
        trackPurchase: function(orderId, revenue, currency = 'USD', products = []) {
            this.track('purchase', {
                order_id: orderId,
                revenue: revenue,
                currency: currency,
                custom_properties: {
                    products: products,
                    product_count: products.length
                }
            });
        },

        trackAddToCart: function(productId, category, value, quantity = 1) {
            this.track('add_to_cart', {
                product_id: productId,
                product_category: category,
                revenue: value,
                quantity: quantity
            });
        },

        trackSignUp: function(method = 'email') {
            this.track('sign_up', {
                custom_properties: {
                    signup_method: method
                }
            });
        },

        // Manual flush
        flush: function() {
            eventQueue.flush();
        },

        // Get current session info
        getSessionInfo: function() {
            return {
                sessionId: sessionManager.sessionId,
                userId: userManager.userId,
                anonymousId: userManager.anonymousId,
                visitId: sessionManager.visitId,
                pageViews: sessionManager.pageViews,
                sessionDuration: sessionManager.getSessionDuration()
            };
        }
    };

    // Auto-initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            window.analytics.init();
        });
    } else {
        window.analytics.init();
    }

})(window, document);