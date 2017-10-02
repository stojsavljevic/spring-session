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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.SerializationServiceSupport;

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
import org.springframework.session.hazelcast.entryprocessor.DeleteSessionEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.DoesSessionExistEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.GetAttributeEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.GetAttributeNamesEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.GetCTEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.GetLATEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.GetSessionEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.SessionState;
import org.springframework.session.hazelcast.entryprocessor.SetCTEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.SetLATEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.SetSessionEntryProcessor;
import org.springframework.session.hazelcast.entryprocessor.UpdateAttributeEntryProcessor;
import org.springframework.util.Assert;

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
 * IMap{@code <String, MapSession>} sessions = hazelcastInstance
 *         .getMap("spring:session:sessions");
 *
 * HazelcastSessionRepository sessionRepository =
 *         new HazelcastSessionRepository(sessions);
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
 * config.getMapConfig("spring:session:sessions")
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
		EntryAddedListener<String, MapSession>,
		EntryEvictedListener<String, MapSession>,
		EntryRemovedListener<String, MapSession> {

	/**
	 * The principal name custom attribute name.
	 */
	public static final String PRINCIPAL_NAME_ATTRIBUTE = "principalName";

	private static final Log logger = LogFactory.getLog(HazelcastSessionRepository.class);

	private final IMap<String, SessionState> sessions;
	
	// FIXME Alex
	private volatile SerializationServiceSupport sss;

	private HazelcastFlushMode hazelcastFlushMode = HazelcastFlushMode.ON_SAVE;

	private ApplicationEventPublisher eventPublisher = new ApplicationEventPublisher() {

		public void publishEvent(ApplicationEvent event) {
		}

		public void publishEvent(Object event) {
		}

	};

	/**
	 * If non-null, this value is used to override
	 * {@link MapSession#setMaxInactiveInterval(Duration)}.
	 */
	private Integer defaultMaxInactiveInterval;

	private String sessionListenerId;

	public HazelcastSessionRepository(IMap<String, SessionState> sessions, HazelcastInstance hazelcastInstance) {
		Assert.notNull(sessions, "Sessions IMap must not be null");
		this.sessions = sessions;
		this.sss = (SerializationServiceSupport) hazelcastInstance;
	}

	@PostConstruct
	private void init() {
		this.sessionListenerId = this.sessions.addEntryListener(this, true);
	}

	@PreDestroy
	private void close() {
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
	 * Sets the Hazelcast flush mode. Default flush mode is
	 * {@link HazelcastFlushMode#ON_SAVE}.
	 * @param hazelcastFlushMode the new Hazelcast flush mode
	 */
	public void setHazelcastFlushMode(HazelcastFlushMode hazelcastFlushMode) {
		Assert.notNull(hazelcastFlushMode, "HazelcastFlushMode cannot be null");
		this.hazelcastFlushMode = hazelcastFlushMode;
	}

	public HazelcastSession createSession() {
		HazelcastSession result = new HazelcastSession();
		if (this.defaultMaxInactiveInterval != null) {
			result.setMaxInactiveInterval(
					Duration.ofSeconds(this.defaultMaxInactiveInterval));
		}
		return result;
	}

	public void save(HazelcastSession session) {
//		if (!session.getId().equals(session.originalId)) {
//			this.sessions.remove(session.originalId);
//			session.originalId = session.getId();
//		}
//		if (session.isChanged()) {
//			this.sessions.put(session.getId(), session.getDelegate(),
//					session.getMaxInactiveInterval().getSeconds(), TimeUnit.SECONDS);
//			session.markUnchanged();
//		}
		
		if(!session.isImmediate() && session.isChanged()) {
			SetSessionEntryProcessor entryProcessor = new SetSessionEntryProcessor(session.getDelegate());
	        executeOnKey(session.sessionId, entryProcessor);
		}
		
	}

	public HazelcastSession findById(String id) {
//		MapSession saved = this.sessions.get(id);
//		if (saved == null) {
//			return null;
//		}
//		if (saved.isExpired()) {
//			deleteById(saved.getId());
//			return null;
//		}
		
		DoesSessionExistEntryProcessor entryProcessor = new DoesSessionExistEntryProcessor();
		boolean exist = (boolean) executeOnKey(id, entryProcessor);
		if(!exist) {
			return null;
		}
		return new HazelcastSession(id);
	}

	public void deleteById(String id) {
		this.sessions.remove(id);
	}

	public Map<String, HazelcastSession> findByIndexNameAndIndexValue(
			String indexName, String indexValue) {
		if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
			return Collections.emptyMap();
		}
//		Collection<MapSession> sessions = this.sessions.values(
//				Predicates.equal(PRINCIPAL_NAME_ATTRIBUTE, indexValue));
//		Map<String, HazelcastSession> sessionMap = new HashMap<>(
//				sessions.size());
//		for (MapSession session : sessions) {
//			sessionMap.put(session.getId(), new HazelcastSession(session));
//		}
//		return sessionMap;
		// FIXME Alex
		return Collections.emptyMap();
	}

	public void entryAdded(EntryEvent<String, MapSession> event) {
		if (logger.isDebugEnabled()) {
			logger.debug("Session created with id: " + event.getValue().getId());
		}
//		this.eventPublisher.publishEvent(new SessionCreatedEvent(this, event.getValue()));
	}

	public void entryEvicted(EntryEvent<String, MapSession> event) {
		if (logger.isDebugEnabled()) {
			logger.debug("Session expired with id: " + event.getOldValue().getId());
		}
//		this.eventPublisher
//				.publishEvent(new SessionExpiredEvent(this, event.getOldValue()));
	}

	public void entryRemoved(EntryEvent<String, MapSession> event) {
		if (logger.isDebugEnabled()) {
			logger.debug("Session deleted with id: " + event.getOldValue().getId());
		}
//		this.eventPublisher
//				.publishEvent(new SessionDeletedEvent(this, event.getOldValue()));
	}
	
	Object executeOnKey(String sessionId, EntryProcessor<String, SessionState> processor) {
		return sessions.executeOnKey(sessionId, processor);
	}

	
	/**
	 * A custom implementation of {@link Session} that uses a {@link MapSession} as the
	 * basis for its mapping. It keeps track if changes have been made since last save.
	 *
	 * @author Aleksandar Stojsavljevic
	 */
	final class HazelcastSession implements Session {

		private boolean changed;
		private String originalId;
		private String sessionId;
		
		private SessionState delegate;

		/**
		 * Creates a new instance ensuring to mark all of the new attributes to be
		 * persisted in the next save operation.
		 */
		HazelcastSession() {
			this(UUID.randomUUID().toString());
			
			setCreationTime();
			this.changed = true;
			//flushImmediateIfNecessary();
		}

		/**
		 * Creates a new instance from the provided {@link MapSession}.
		 * @param cached the {@link MapSession} that represents the persisted session that
		 * was retrieved. Cannot be {@code null}.
		 */
		HazelcastSession(String id) {
			Assert.notNull(id, "ID cannot be null");
//			this.originalId = cached.getId();
			this.sessionId = id;
			if(!isImmediate()) {
				GetSessionEntryProcessor entryProcessor = new GetSessionEntryProcessor();
				this.delegate = (SessionState) executeOnKey(this.sessionId, entryProcessor);
			}
			if(this.delegate == null) {
				this.delegate = new SessionState();
			}
		}

		public void setLastAccessedTime(Instant lastAccessedTime) {
			if(isImmediate()) {
				SetLATEntryProcessor entryProcessor = new SetLATEntryProcessor(lastAccessedTime);
		        executeOnKey(this.sessionId, entryProcessor);
			} else {
				this.changed = true;
				delegate.setLastAccessedTime(lastAccessedTime);
			}
		}

		public boolean isExpired() {
			// FIXME Alex
//			return this.delegate.isExpired();
			return false;
		}

		public Instant getCreationTime() {
			Instant inst;
			if(isImmediate()) {
				GetCTEntryProcessor entryProcessor = new GetCTEntryProcessor();
				inst = (Instant) executeOnKey(this.sessionId, entryProcessor);
			} else {
				inst = this.delegate.getCreationTime();
			}
			return inst != null ? inst : Instant.now();
		}
		
		public void setCreationTime() {
			Instant inst = Instant.now();
			if(isImmediate()) {
				SetCTEntryProcessor entryProcessor = new SetCTEntryProcessor(inst);
				executeOnKey(this.sessionId, entryProcessor);
			} else {
				this.delegate.setCreationTime(inst);
			}
		}

		public String getId() {
			return this.sessionId;
		}

		public String changeSessionId() {
			if (isImmediate()) {
				this.delegate = (SessionState) executeOnKey(this.sessionId, new GetSessionEntryProcessor());
				
				this.sessionId = UUID.randomUUID().toString();
				
				executeOnKey(this.sessionId, new SetSessionEntryProcessor(this.delegate));
			} else {
				this.sessionId = UUID.randomUUID().toString();
			}
			return this.sessionId;
		}

		public Instant getLastAccessedTime() {
			Instant inst;
			if (isImmediate()) {
				GetLATEntryProcessor entryProcessor = new GetLATEntryProcessor();
				inst = (Instant) executeOnKey(this.sessionId, entryProcessor);
			} else {
				inst = this.delegate.getLastAccessedTime();
			}
	        return inst != null ? inst : Instant.now();
		}

		public void setMaxInactiveInterval(Duration interval) {
			// FIXME Alex
			if(isImmediate()) {
				
			} else {
				this.delegate.setMaxInactiveInterval(interval);
				this.changed = true;
			}
			//flushImmediateIfNecessary();
		}

		public Duration getMaxInactiveInterval() {
			// FIXME Alex
//			return this.delegate.getMaxInactiveInterval();
			return Duration.ZERO;
		}

		public Object getAttribute(String attributeName) {
			if (isImmediate()) {
				GetAttributeEntryProcessor entryProcessor = new GetAttributeEntryProcessor(attributeName);
		        return executeOnKey(this.sessionId, entryProcessor);
			} else {
				return sss.getSerializationService().toObject(this.delegate.getAttribute(attributeName));
			}
		}

		public Set<String> getAttributeNames() {
			if (isImmediate()) {
				Object attributeNames = executeOnKey(this.sessionId, new GetAttributeNamesEntryProcessor());
				if(attributeNames == null) {
					return Collections.emptySet();
				}
				return (Set<String>) attributeNames;
			} else {
				return this.delegate.getAttributes().keySet();
			}
		}

		public void setAttribute(String attributeName, Object attributeValue) {
			Data dataValue = (attributeValue == null) ? null : sss.getSerializationService().toData(attributeValue);
			if (isImmediate()) {
				UpdateAttributeEntryProcessor sessionUpdateProcessor = new UpdateAttributeEntryProcessor(attributeName, dataValue);
				executeOnKey(sessionId, sessionUpdateProcessor);
			} else {
				this.delegate.setAttribute(attributeName, dataValue);
				this.changed = true;
			}
		}

		public void removeAttribute(String attributeName) {
			if (isImmediate()) {
				UpdateAttributeEntryProcessor sessionUpdateProcessor = new UpdateAttributeEntryProcessor(attributeName, null);
				executeOnKey(sessionId, sessionUpdateProcessor);
			} else {
				this.delegate.setAttribute(attributeName, null);
				this.changed = true;
			}
		}

		boolean isChanged() {
			return this.changed;
		}

		void markUnchanged() {
			this.changed = false;
		}

		SessionState getDelegate() {
			return this.delegate;
		}

//		private void flushImmediateIfNecessary() {
//			if (isImmediate()) {
//				HazelcastSessionRepository.this.save(this);
//			}
//		}
		
		private boolean isImmediate() {
			return HazelcastSessionRepository.this.hazelcastFlushMode == HazelcastFlushMode.IMMEDIATE;
		}
	}

}
