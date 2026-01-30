package com.heapik.slot.outbox.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heapik.slot.commonsevent.domain.Event;
import com.heapik.slot.commonsevent.domain.outbox.EventOutbox;
import com.heapik.slot.commonsevent.ports.outbox.EventOutboxRepositoryPort;
import com.heapik.slot.outbox.domain.EventOutboxOrm;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class EventOutboxRepositoryAdapter implements EventOutboxRepositoryPort {

    private final EventOutboxJpaRepository eventOutboxJpaRepository;
    private final ObjectMapper objectMapper;

    public EventOutboxRepositoryAdapter(EventOutboxJpaRepository eventOutboxJpaRepository, ObjectMapper objectMapper) {
        this.eventOutboxJpaRepository = eventOutboxJpaRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public List<EventOutbox> findAllUnpublishedEvents(Instant cursor, UUID tieBreaker, int limit, int retryCount) {
        return eventOutboxJpaRepository.findUnpublishedEventOutboxesByRetryCountLessThan(retryCount, cursor, tieBreaker, Pageable.ofSize(limit));
    }

    @Override
    public void save(List<EventOutbox> list) {
        var mapped = list.stream().map(EventOutboxOrm::fromDomain).toList();
        eventOutboxJpaRepository.saveAll(mapped);
    }

    @Override
    public void saveEvent(Event<?> event) {
        var outbox = new EventOutboxOrm(event.eventType(), objectMapper.valueToTree(event.payload()), event.occurredAt());
        eventOutboxJpaRepository.save(outbox);
    }
}
