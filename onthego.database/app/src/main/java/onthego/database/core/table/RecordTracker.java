package onthego.database.core.table;

import java.util.Optional;

public interface RecordTracker<T> {
    Optional<T> getNewPosition(T oldPosition);

    void setNewPosition(T oldPosition, T newPosition);

    boolean contains(T oldPosition);
}
