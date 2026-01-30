package com.heapik.slot.outbox.job;

import com.heapik.slot.commonsevent.domain.Event;
import com.heapik.slot.commonsevent.domain.outbox.EventOutbox;
import com.heapik.slot.commonsevent.ports.publisher.EventPublisherPort;
import com.heapik.slot.outbox.autoconfig.EventOutboxProperties;
import com.heapik.slot.outbox.service.EventOutboxPublisherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;

public class EventOutboxPublisherCron {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventOutboxPublisherCron.class);

    private final EventOutboxProperties properties;

    private final EventOutboxPublisherService eventOutboxPublisherService;
    private final EventPublisherPort eventPublisherPort;

    public EventOutboxPublisherCron(EventOutboxProperties properties, EventOutboxPublisherService eventOutboxPublisherService, EventPublisherPort eventPublisherPort) {
        this.properties = properties;
        this.eventOutboxPublisherService = eventOutboxPublisherService;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Scheduled(cron = "${slot.event.outbox.scheduling-cron-expression}")
    public void publishOutboxEvents() {

        while (true) {
            var pagination = eventOutboxPublisherService.getCursor();
            var batch = eventOutboxPublisherService.getBatch(pagination, properties.getBatchSize());

            if (batch.isEmpty()) {
                LOGGER.info("No unpublished events found. Finished processing outbox events.");
                return;
            }

            var success = new ArrayList<EventOutbox>();
            LOGGER.info("Publishing {} events for cursor {}, {}", batch.size(), pagination.getCursor(), pagination.getTieBreaker());
            for (var event : batch) {
                var send = new Event<>(
                        event.getEventType(),
                        event.getOccurredAt(),
                        event.getPayload()
                );

                try {
                    eventPublisherPort.publish(send);
                    event.published();
                    success.add(event);
                    LOGGER.debug("Succeeded to publish event {}, outbox id: {}",
                            event.getEventType(),
                            event.getId().toString());
                } catch (Exception e) {
                    LOGGER.error("Failed to publish event {}, retry no {}, outbox id {}",
                            event.getEventType(),
                            event.getRetryCount() + 1,
                            event.getId().toString(),
                            e);
                    event.incrementRetryCount();
                    event.setErrorMessage(e.getMessage());
                }
            }

            eventOutboxPublisherService.updateBatch(batch);

            if (!success.isEmpty()) {
                LOGGER.debug("Updating last successful cursor to {} {}", success.getLast().getId(), success.getLast().getOccurredAt());
                eventOutboxPublisherService.updateLastSuccessfulCursor(pagination, success.getLast());
            }

            if (batch.size() < properties.getBatchSize()) {
                LOGGER.info("Finished processing outbox events.");
                return;
            }
        }
    }
}
