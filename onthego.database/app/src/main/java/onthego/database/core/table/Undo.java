package onthego.database.core.table;

import java.util.Map;

public interface Undo {

    void setRecordPosTracker(Map<Long, Long> recordPosTracker);

    void execute();

}
