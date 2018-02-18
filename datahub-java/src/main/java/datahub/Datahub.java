package ru.oseval.datahub.j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.oseval.datahub.EntityFacadeInt;
import ru.oseval.datahub.data.j.Data;
import ru.oseval.datahub.data.j.DataOps;

public class Datahub {
    public interface Storage {
        <C> void increase(String entityId, C dataClock, Comparator<C> cmp, Function<Void, Void> callback);
        <C> void getLastClock(String entityId, Function<Optional<C>, Void> callback);
    }

    public static Function<Void, Void> emptyCallback = x -> null;

    private Storage storage;
    private Logger log;

    private Map<String, EntityFacadeInt> facades = new ConcurrentHashMap<>();
    private Map<String, Set<String>> subscribers = new ConcurrentHashMap<>(); // facade -> subscribers
    private Map<String, Set<String>> relations = new ConcurrentHashMap<>(); // facade -> relations
    // subscribers to which changes will be sent anyway, but without clock sync
    private Map<String, Set<EntityFacadeInt>> forcedSubscribers = new ConcurrentHashMap<>();

    public Datahub(Storage _storage) {
        this.log = LoggerFactory.getLogger(this.getClass());
        this.storage = new MemoryFallbackStorage(_storage);
    }

    public void register(EntityFacadeInt facade,
                         final Object lastClock,
                         Map<String, Object> relationClocks,
                         Function<Void, Void> callback) {
        facades.put(facade.getEntity().getId(), facade);

        // this facade depends on that relations
        for (Map.Entry<String, Object> entry : relationClocks.entrySet()) {
            subscribe(facade, entry.getKey(), Optional.of(entry.getValue()));
        }

        // sync registered entity clock
        storage.getLastClock(facade.getEntity().getId(), lastStoredClockOpt -> {
            DataOps fops = facade.getEntity().getOps();

            lastStoredClockOpt.flatMap(clock -> fops.matchClock(clock)).map(lastStoredClock -> {
                if (fops.getOrdering().compare(lastClock, lastStoredClock) > 0) {
                    facade.getUpdatesFrom(lastStoredClock, d -> {
                        dataUpdated(facade.getEntity().getId(), d, emptyCallback);
                        return null;
                    });
                }

                return null;
            });

            if (!lastStoredClockOpt.isPresent()) {
                storage.increase(
                        facade.getEntity().getId(), lastClock, facade.getEntity().getOps().getOrdering(), callback
                );
            }

            return null;
        });
    }

    public void setForcedSubscribers(String entityId, Set<EntityFacadeInt> forced, Function<Void, Void> callback) {
        forcedSubscribers.put(entityId, forced);
    }

    public void dataUpdated(String entityId, Data _data, Function<Void, Void> callback) {
        if (!facades.containsKey(entityId)) {
            log.error("Facade with id={} is not registered", entityId);
        } else {
            EntityFacadeInt facade = facades.get(entityId);
            DataOps ops = facade.getEntity().getOps();
            Optional<DataOps.D> dataOpt = ops.matchData(_data);

            if (!dataOpt.isPresent()) {
                log.error("Entity {} with taken facade {} does not match data {}",
                        entityId,
                        facade.getEntity().getId(),
                        _data.getClass().getName()
                );
            } else {
                DataOps.D data = dataOpt.get();

                // who subscribed on that facade
                Set<String> curSubscribers = subscribers.get(entityId);
                if (curSubscribers != null) {
                    for (String subs : curSubscribers) {
                        if (facades.containsKey(subs)) {
                            sendChangeToOne(facades.get(subs), facade.getEntity(), data);
                        }
                    }
                }

                // send changes to forced subscribers
                Set<EntityFacadeInt> forced = forcedSubscribers.get(entityId);
                if (forced != null) {
                    for (EntityFacadeInt f : forced) {
                        sendChangeToOne(f, facade.getEntity(), data);
                    }
                }

                // the facades on which that facade depends
                Set<String> newRelations = ops.getRelations(data);
                Set<String> rels = relations.get(entityId);

                for (String relationId : newRelations) {
                    if (rels == null || !rels.contains(relationId)) {
                        subscribe(facade, relationId, Optional.empty());
                    }
                }

                for (String relationId : rels) {
                    if (!newRelations.contains(relationId)) {
                        unsubscribe(facade, relationId);
                    }
                }

                storage.increase(entityId, data.getClock(), facade.getEntity().getOps().getOrdering(), callback);
            }
        }
    }

    public void syncRelationClocks(String entityId, Map<String, Object> relationClocks) {
        if (facades.containsKey(entityId)) {
            EntityFacadeInt facade = facades.get(entityId);
            for (Map.Entry<String, Object> rel : relationClocks.entrySet()) {
                syncRelation(facade, rel.getKey(), Optional.of(rel.getValue()));
            }
        }
    }

    public void subscribeApproved(EntityFacadeInt facade, String relationId, Optional<Object> lastKnownDataClockOpt) {
        subscribers.computeIfAbsent(relationId, x -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                .add(facade.getEntity().getId());

        relations.computeIfAbsent(
                facade.getEntity().getId(),
                x -> Collections.newSetFromMap(new ConcurrentHashMap<>())
        ).add(relationId);

        syncRelation(facade, relationId, lastKnownDataClockOpt);
    }

    protected void sendChangeToOne(EntityFacadeInt to, Entity related, Data relatedData) {
        to.onUpdate(related.getId(), relatedData);
    }

    private void syncRelation(EntityFacadeInt facade, String relatedId, Optional<Object> lastKnownDataClockOpt) {
        if (facades.containsKey(relatedId)) {
            EntityFacadeInt related = facades.get(relatedId);
            log.debug(
                    "Syncing entity {} on {} with last known related clock {}",
                    facade.getEntity().getId(), relatedId, lastKnownDataClockOpt
            );

            DataOps relops = related.getEntity().getOps();

            storage.getLastClock(relatedId, clockOpt -> {
                relops.matchClock(clockOpt).ifPresent(lastClock -> {
                    Object lastKnownDataClock = lastKnownDataClockOpt
                            .flatMap(c -> relops.matchClock(c))
                            .orElse(relops.getZero().getClock());

                    log.debug("lastClock {}, lastKnownClock {}, {}",
                            lastClock, lastKnownDataClock, relops.getOrdering().compare(lastClock, lastKnownDataClock) > 0
                    );

                    if (relops.getOrdering().compare(lastClock, lastKnownDataClock) > 0) {
                        related.getUpdatesFrom(lastKnownDataClock, d -> {
                            sendChangeToOne(facade, related.getEntity(), d);
                            return null;
                        });
                    }
                });

                return null;
            });
        }
    }

    private void subscribe(EntityFacadeInt facade, String relationId, Optional<Object> lastKnownDataClockOpt) {
        log.debug("subscribe {}, {}, {}", facade.getEntity().getId(), relationId, facades.get(relationId));

        if (facades.containsKey(relationId)) {
            EntityFacadeInt relation = facades.get(relationId);
            relation.requestForApprove(facade.getEntity(), approved -> {
                if (approved) {
                    subscribeApproved(facade, relationId, lastKnownDataClockOpt);
                } else {
                    log.warn("Failed to subscribe on {} due untrusted kind {}{}",
                            relationId, facade.getEntity().getOps().getKind());
                }

                return null;
            });
        }
    }

    private void unsubscribe(EntityFacadeInt facade, String relatedId) {
        log.debug("Unsubscribe entity {} from related {}", facade.getEntity().getId(), relatedId);
        Set<String> newRelatedSubscriptions = subscribers.get(relatedId);

        if (newRelatedSubscriptions != null) {
            newRelatedSubscriptions.remove(facade.getEntity().getId());

            if (newRelatedSubscriptions.isEmpty()) {
                subscribers.remove(relatedId);
            }
        }

        Set<String> newRelations = relations.get(facade.getEntity().getId());
        if (newRelations != null) {
            newRelations.remove(relatedId);

          if (newRelations.isEmpty()) {
              relations.remove(facade.getEntity().getId());
          }
        }
    }
}