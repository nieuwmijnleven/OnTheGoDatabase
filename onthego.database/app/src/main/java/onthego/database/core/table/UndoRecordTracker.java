package onthego.database.core.table;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class UndoRecordTracker<T> implements RecordTracker<T> {

    private final Map<T, T> recordRemap = new HashMap<>();

    private UndoRecordTracker() {}

    public static <K> UndoRecordTracker<K> create() {
        return new UndoRecordTracker<>();
    }

    @Override
    public Optional<T> getNewPosition(T oldPosition) {
        return Optional.ofNullable(recordRemap.get(oldPosition));
    }

    @Override
    public void setNewPosition(T oldPosition, T newPosition) {
        recordRemap.put(oldPosition, newPosition);
    }

    @Override
    public boolean contains(T oldPosition) {
        return recordRemap.containsKey(oldPosition);
    }
}
