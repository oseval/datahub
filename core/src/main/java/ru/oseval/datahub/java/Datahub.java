package ru.oseval.datahub.java;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.oseval.datahub.data.java.Data;
import ru.oseval.datahub.data.java.DataOps;

public abstract class Datahub {
    public interface Storage {
        void register(String entityId, Object dataClock, Function<Void, Void> callback);
        void change(String entityId, Object dataClock, Function<Void, Void> callback);
        void getLastClock(String entityId, Function<Optional<Object>, Void> callback);
    }

    private Storage storage;
    private Logger log;

    private Map<String, EntityFacade> facades = new HashMap<>();
    private Map<String, Set<String>> subscriptions = new HashMap<>(); // facade -> subscriptions
    private Map<String, Set<String>> reverseSubscriptions = new HashMap<>(); // facade -> related

    public Datahub(Storage _storage) {
        this.log = LoggerFactory.getLogger(this.getClass());
        this.storage = new MemoryFallbackStorage(_storage);
    }

    public void register(EntityFacade facade,
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
                        dataUpdated(facade.getEntity().getId(), d, x -> null);
                        return null;
                    });
                }

                return null;
            });

            return null;
        });

        storage.register(facade.getEntity().getId(), lastClock, callback);
    }

    public void dataUpdated(String entityId, Data _data, Function<Void, Void> callback) {
        if (!facades.containsKey(entityId)) {
            log.error("Facade with id={} is not registered", entityId);
        } else {
            EntityFacade facade = facades.get(entityId);
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
                Set<String> subscribers = subscriptions.getOrDefault(entityId, new HashSet<>());
                for (String subs : subscribers) {
                    if (facades.containsKey(subs)) {
                        sendChangeToOne(facades.get(subs), facade.getEntity(), data);
                    }
                }

                // the facades on which that facade depends
                Set<String> relatedFacades = ops.getRelations(data);
                Set<String> reverseSubs = reverseSubscriptions.getOrDefault(entityId, new HashSet<>());

                for (String relationId : relatedFacades) {
                    if (!reverseSubs.contains(relationId)) {
                        subscribe(facade, relationId, Optional.empty());
                    }
                }

                for (String reverseId : reverseSubs) {
                    if (!relatedFacades.contains(reverseId)) {
                        unsubscribe(facade, reverseId);
                    }
                }

                storage.change(entityId, data.getClock(), callback);
            }
        }
    }

    public void syncRelationClocks(String entityId, Map<String, Object> relationClocks) {
        if (facades.containsKey(entityId)) {
            EntityFacade facade = facades.get(entityId);
            for (Map.Entry<String, Object> rel : relationClocks.entrySet()) {
                syncRelation(facade, rel.getKey(), Optional.of(rel.getValue()));
            }
        }
    }

    public void subscribeApproved(EntityFacade facade, String relationId, Optional<Object> lastKnownDataClockOpt) {
        Set<String> relatedSubscriptions = subscriptions.getOrDefault(relationId, new HashSet<>());
        relatedSubscriptions.add(facade.getEntity().getId());
        subscriptions.put(relationId, relatedSubscriptions);

        Set<String> rSubscriptions = reverseSubscriptions.getOrDefault(facade.getEntity().getId(), new HashSet<>());
        rSubscriptions.add(relationId);
        reverseSubscriptions.put(facade.getEntity().getId(), rSubscriptions);
    }

    protected void sendChangeToOne(EntityFacade to, Entity related, Data relatedData) {
        to.onUpdate(related.getId(), relatedData);
    }

    private void syncRelation(EntityFacade facade, String relatedId, Optional<Object> lastKnownDataClockOpt) {
        if (facades.containsKey(relatedId)) {
            EntityFacade related = facades.get(relatedId);
            log.debug(
                    "Subscribe entity {} on {} with last known related clock {}",
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

  private void subscribe(EntityFacade facade, String relationId, Optional<Object> lastKnownDataClockOpt) {
    log.debug("subscribe {}, {}, {}", facade.getEntity().getId(), relationId, facades.get(relationId));

    if (facades.containsKey(relationId)) {
        EntityFacade relation = facades.get(relationId);
        relation.requestForApprove(facade.getEntity(), approved -> {
            if (approved) {
                subscribeApproved(facade, relationId, lastKnownDataClockOpt);
            } else {
                log.warn(
                        "Failed to subscribe on {} due untrusted kind {}{}",
                        relationId, facade.getEntity().getOps().getKind());
            }

            return null;
        });
    }
  }

  private void unsubscribe(EntityFacade facade, String relatedId) {
    log.debug("Unsubscribe entity {} from related {}", facade.getEntity().getId(), relatedId);
    Set<String> newRelatedSubscriptions = subscriptions.getOrDefault(relatedId, new HashSet<>());
    newRelatedSubscriptions.remove(facade.getEntity().getId());

    if (newRelatedSubscriptions.isEmpty()) {
        subscriptions.remove(relatedId);
    }
  }
}