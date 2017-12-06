/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.session.hazelcast;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.events.AbstractSessionEvent;
import org.springframework.session.events.SessionCreatedEvent;
import org.springframework.session.events.SessionDeletedEvent;
import org.springframework.session.events.SessionExpiredEvent;
import org.springframework.session.hazelcast.ep.GetSessionEntryProcessor;
import org.springframework.session.hazelcast.ep.SessionState;
import org.springframework.session.hazelcast.ep.SessionUpdateEntryProcessor;
import org.springframework.util.Assert;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * A {@link org.springframework.session.SessionRepository} implementation that stores
 * sessions in Hazelcast's distributed {@link IMap}.
 *
 * <p>
 * An example of how to create a new instance can be seen below:
 *
 * <pre class="code">
 * Config config = new Config();
 *
 * // ... configure Hazelcast ...
 *
 * HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
 *
 * HazelcastSessionRepository sessionRepository =
 *         new HazelcastSessionRepository(hazelcastInstance);
 * </pre>
 *
 * In order to support finding sessions by principal name using
 * {@link #findByIndexNameAndIndexValue(String, String)} method, custom configuration of
 * {@code IMap} supplied to this implementation is required.
 *
 * The following snippet demonstrates how to define required configuration using
 * programmatic Hazelcast Configuration:
 *
 * <pre class="code">
 * MapAttributeConfig attributeConfig = new MapAttributeConfig()
 *         .setName(HazelcastSessionRepository.PRINCIPAL_NAME_ATTRIBUTE)
 *         .setExtractor(PrincipalNameExtractor.class.getName());
 *
 * Config config = new Config();
 *
 * config.getMapConfig(HazelcastSessionRepository.DEFAULT_SESSION_MAP_NAME)
 *         .addMapAttributeConfig(attributeConfig)
 *         .addMapIndexConfig(new MapIndexConfig(
 *                 HazelcastSessionRepository.PRINCIPAL_NAME_ATTRIBUTE, false));
 *
 * Hazelcast.newHazelcastInstance(config);
 * </pre>
 *
 * This implementation listens for events on the Hazelcast-backed SessionRepository and
 * translates those events into the corresponding Spring Session events. Publish the
 * Spring Session events with the given {@link ApplicationEventPublisher}.
 *
 * <ul>
 * <li>entryAdded - {@link SessionCreatedEvent}</li>
 * <li>entryEvicted - {@link SessionExpiredEvent}</li>
 * <li>entryRemoved - {@link SessionDeletedEvent}</li>
 * </ul>
 *
 * @author Vedran Pavic
 * @author Tommy Ludwig
 * @author Mark Anderson
 * @author Aleksandar Stojsavljevic
 * @since 1.3.0
 */
public class HazelcastSessionRepository implements
		FindByIndexNameSessionRepository<HazelcastSessionRepository.HazelcastSession>,
		EntryAddedListener<String, SessionState>, EntryEvictedListener<String, SessionState>,
		EntryRemovedListener<String, SessionState> {

	/**
	 * The default name of map used by Spring Session to store sessions.
	 */
	public static final String DEFAULT_SESSION_MAP_NAME = "spring:session:sessions";

	/**
	 * The principal name custom attribute name.
	 */
	public static final String PRINCIPAL_NAME_ATTRIBUTE = "principalName";

	private static final Log logger = LogFactory.getLog(HazelcastSessionRepository.class);

	private final HazelcastInstance hazelcastInstance;

	private ApplicationEventPublisher eventPublisher = new ApplicationEventPublisher() {

		@Override
		public void publishEvent(ApplicationEvent event) {
		}

		@Override
		public void publishEvent(Object event) {
		}

	};

	/**
	 * If non-null, this value is used to override
	 * {@link MapSession#setMaxInactiveInterval(Duration)}.
	 */
	private Integer defaultMaxInactiveInterval;

	private String sessionMapName = DEFAULT_SESSION_MAP_NAME;

	private HazelcastFlushMode hazelcastFlushMode = HazelcastFlushMode.ON_SAVE;

	private IMap<String, SessionState> sessions;

	private String sessionListenerId;

	public static final String SESSION_ATTRIBUTE_PREFIX = "SPRINGSESSION::";

	public static final String SESSION_ATTRIBUTE_LAT = SESSION_ATTRIBUTE_PREFIX + "LAT";

	public static final String SESSION_ATTRIBUTE_CT = SESSION_ATTRIBUTE_PREFIX + "CT";

	public static final String SESSION_ATTRIBUTE_DURATION = SESSION_ATTRIBUTE_PREFIX + "DURATION";

	public HazelcastSessionRepository(HazelcastInstance hazelcastInstance) {
		Assert.notNull(hazelcastInstance, "HazelcastInstance must not be null");
		this.hazelcastInstance = hazelcastInstance;
	}

	@PostConstruct
	public void init() {
		this.sessions = this.hazelcastInstance.getMap(this.sessionMapName);
		this.sessionListenerId = this.sessions.addEntryListener(this, true);
	}

	@PreDestroy
	public void close() {
		this.sessions.removeEntryListener(this.sessionListenerId);
	}

	/**
	 * Sets the {@link ApplicationEventPublisher} that is used to publish
	 * {@link AbstractSessionEvent session events}. The default is to not publish session
	 * events.
	 *
	 * @param applicationEventPublisher the {@link ApplicationEventPublisher} that is used
	 * to publish session events. Cannot be null.
	 */
	public void setApplicationEventPublisher(
			ApplicationEventPublisher applicationEventPublisher) {
		Assert.notNull(applicationEventPublisher,
				"ApplicationEventPublisher cannot be null");
		this.eventPublisher = applicationEventPublisher;
	}

	/**
	 * Set the maximum inactive interval in seconds between requests before newly created
	 * sessions will be invalidated. A negative time indicates that the session will never
	 * timeout. The default is 1800 (30 minutes).
	 * @param defaultMaxInactiveInterval the maximum inactive interval in seconds
	 */
	public void setDefaultMaxInactiveInterval(Integer defaultMaxInactiveInterval) {
		this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
	}

	/**
	 * Set the name of map used to store sessions.
	 * @param sessionMapName the session map name
	 */
	public void setSessionMapName(String sessionMapName) {
		Assert.hasText(sessionMapName, "Map name must not be empty");
		this.sessionMapName = sessionMapName;
	}

	/**
	 * Sets the Hazelcast flush mode. Default flush mode is
	 * {@link HazelcastFlushMode#ON_SAVE}.
	 * @param hazelcastFlushMode the new Hazelcast flush mode
	 */
	public void setHazelcastFlushMode(HazelcastFlushMode hazelcastFlushMode) {
		Assert.notNull(hazelcastFlushMode, "HazelcastFlushMode cannot be null");
		this.hazelcastFlushMode = hazelcastFlushMode;
	}

	@Override
	public HazelcastSession createSession() {
		HazelcastSession result = new HazelcastSession();
		if (this.defaultMaxInactiveInterval != null) {
			result.setMaxInactiveInterval(
					Duration.ofSeconds(this.defaultMaxInactiveInterval));
		}
		return result;
	}

	@Override
	public void save(HazelcastSession session) {
		if (!session.getId().equals(session.originalId)) {
			this.sessions.remove(session.originalId);
			session.originalId = session.getId();
		}
		if (session.isNew) {
			session.delta.put(SESSION_ATTRIBUTE_CT,
					getSerializationService().toData(session.getCreationTime()));
			this.sessions.executeOnKey(session.getId(),
					new SessionUpdateEntryProcessor(new SessionState(session.delta)));
		}
		else if (session.changed) {
			this.sessions.executeOnKey(session.getId(),
					new SessionUpdateEntryProcessor(new SessionState(session.delta)));
		}
		session.clearFlags();
	}

	@Override
	public HazelcastSession findById(String id) {
		Object saved = this.sessions.executeOnKey(id, new GetSessionEntryProcessor());
		if (saved == null) {
			return null;
		}
		// TODO ALex
		// if (saved.isExpired()) {
		// deleteById(saved.getId());
		// return null;
		// }
		return new HazelcastSession(getMapSession((SessionState) saved, id));
	}

	private MapSession getMapSession(SessionState sessionState, String id) {
		if (sessionState == null) {
			return new MapSession(id);
		}
		Iterator<Entry<String, Data>> iterator = sessionState.getAttributes().entrySet().iterator();
		MapSession mapSession = new MapSession();
		while (iterator.hasNext()) {
			Entry<String, Data> entry = iterator.next();
			if (entry.getKey().startsWith(SESSION_ATTRIBUTE_PREFIX)) {
				continue;
			}
			Object value = null;
			if (entry.getValue() != null) {
				value = getSerializationService().toObject(entry.getValue());
			}
			mapSession.setAttribute(entry.getKey(), value);
		}
		mapSession.setId(id);

		mapSession.setLastAccessedTime(getSerializationService()
				.toObject(sessionState.getAttributes().get(SESSION_ATTRIBUTE_LAT)));
		mapSession.setCreationTime(getSerializationService()
				.toObject(sessionState.getAttributes().get(SESSION_ATTRIBUTE_CT)));
		mapSession.setMaxInactiveInterval(getSerializationService()
				.toObject(sessionState.getAttributes().get(SESSION_ATTRIBUTE_DURATION)));
		return mapSession;
	}

	@Override
	public void deleteById(String id) {
		this.sessions.remove(id);
	}

	@Override
	public Map<String, HazelcastSession> findByIndexNameAndIndexValue(String indexName,
			String indexValue) {
		// TODO ALex
		// if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
		// return Collections.emptyMap();
		// }
		// Collection<MapSession> sessions = this.sessions
		// .values(Predicates.equal(PRINCIPAL_NAME_ATTRIBUTE, indexValue));
		// Map<String, HazelcastSession> sessionMap = new
		// HashMap<>(sessions.size());
		// for (MapSession session : sessions) {
		// sessionMap.put(session.getId(), new HazelcastSession(session));
		// }
		// return sessionMap;
		return null;
	}

	@Override
	public void entryAdded(EntryEvent<String, SessionState> event) {
		if (logger.isDebugEnabled()) {
			logger.debug("Session created with id: " + event.getValue().getId());
		}
		this.eventPublisher.publishEvent(
				new SessionCreatedEvent(this, getMapSession(event.getValue(), event.getKey())));
	}

	@Override
	public void entryEvicted(EntryEvent<String, SessionState> event) {
		if (logger.isDebugEnabled()) {
			logger.debug("Session expired with id: " + event.getOldValue().getId());
		}
		this.eventPublisher.publishEvent(
				new SessionExpiredEvent(this, getMapSession(event.getValue(), event.getKey())));
	}

	@Override
	public void entryRemoved(EntryEvent<String, SessionState> event) {
		if (logger.isDebugEnabled()) {
			logger.debug("Session deleted with id: " + event.getOldValue().getId());
		}
		this.eventPublisher.publishEvent(
				new SessionDeletedEvent(this, getMapSession(event.getValue(), event.getKey())));
	}

	private SerializationService getSerializationService() {
		return ((SerializationServiceSupport) this.hazelcastInstance).getSerializationService();
	}

	/**
	 * A custom implementation of {@link Session} that uses a {@link MapSession} as the
	 * basis for its mapping. It keeps track if changes have been made since last save.
	 *
	 * @author Aleksandar Stojsavljevic
	 */
	final class HazelcastSession implements Session {

		private final MapSession delegate;

		private boolean isNew;

		private boolean changed;

		private String originalId;

		private Map<String, Data> delta = new HashMap<>();

		/**
		 * Creates a new instance ensuring to mark all of the new attributes to be
		 * persisted in the next save operation.
		 */
		HazelcastSession() {
			this(new MapSession());
			this.isNew = true;
			flushImmediateIfNecessary();
		}

		/**
		 * Creates a new instance from the provided {@link MapSession}.
		 * @param cached the {@link MapSession} that represents the persisted session that
		 * was retrieved. Cannot be {@code null}.
		 */
		HazelcastSession(MapSession cached) {
			Assert.notNull(cached, "MapSession cannot be null");
			this.delegate = cached;
			this.originalId = cached.getId();
		}

		@Override
		public void setLastAccessedTime(Instant lastAccessedTime) {
			this.delegate.setLastAccessedTime(lastAccessedTime);

			delta.put(SESSION_ATTRIBUTE_LAT, getSerializationService().toData(lastAccessedTime));
			this.changed = true;
			flushImmediateIfNecessary();
		}

		@Override
		public boolean isExpired() {
			return this.delegate.isExpired();
		}

		@Override
		public Instant getCreationTime() {
			return this.delegate.getCreationTime();
		}

		@Override
		public String getId() {
			return this.delegate.getId();
		}

		@Override
		public String changeSessionId() {
			this.isNew = true;
			return this.delegate.changeSessionId();
		}

		@Override
		public Instant getLastAccessedTime() {
			return this.delegate.getLastAccessedTime();
		}

		@Override
		public void setMaxInactiveInterval(Duration interval) {
			this.delegate.setMaxInactiveInterval(interval);

			delta.put(SESSION_ATTRIBUTE_DURATION, getSerializationService().toData(interval));
			this.changed = true;
			flushImmediateIfNecessary();
		}

		@Override
		public Duration getMaxInactiveInterval() {
			return this.delegate.getMaxInactiveInterval();
		}

		@Override
		public <T> T getAttribute(String attributeName) {
			return this.delegate.getAttribute(attributeName);
		}

		@Override
		public Set<String> getAttributeNames() {
			return this.delegate.getAttributeNames();
		}

		@Override
		public void setAttribute(String attributeName, Object attributeValue) {
			this.delegate.setAttribute(attributeName, attributeValue);
			this.delta.put(attributeName, getSerializationService().toData(attributeValue));
			this.changed = true;
			flushImmediateIfNecessary();
		}

		@Override
		public void removeAttribute(String attributeName) {
			this.delegate.removeAttribute(attributeName);
			this.delta.put(attributeName, null);
			this.changed = true;
			flushImmediateIfNecessary();
		}

		MapSession getDelegate() {
			return this.delegate;
		}

		void clearFlags() {
			this.isNew = false;
			this.changed = false;
			this.delta.clear();
		}

		private void flushImmediateIfNecessary() {
			if (HazelcastSessionRepository.this.hazelcastFlushMode == HazelcastFlushMode.IMMEDIATE) {
				HazelcastSessionRepository.this.save(this);
			}
		}

	}
}
