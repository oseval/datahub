package ru.oseval.datahub.java;

import ru.oseval.datahub.data.java.Data;

import java.util.function.Function;

interface EntityFacade {
    Entity getEntity();

    /**
     * Request explicit data difference from entity
     * @param dataClock
     * @return
     */
    void getUpdatesFrom(Object dataClock, Function<Data, Void> callback);

    /**
     * Receives updates of related external data
     * @param relatedId
     * @param relatedData
     * @return
     */
    void onUpdate(String relatedId, Data relatedData);

    /**
     * When an entity is not trust to the relation kind then a subscription must approved
     * @param relation
     * @return
     */
    void requestForApprove(Entity relation, Function<Boolean, Void> callback);
}
