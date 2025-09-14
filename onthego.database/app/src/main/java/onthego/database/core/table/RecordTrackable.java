package onthego.database.core.table;

import java.util.Map;

public interface RecordTrackable<T> {
    void setRecordPosTracker(RecordTracker<T> recordTracker);
}
