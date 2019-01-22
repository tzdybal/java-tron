package org.tron.core.capsule;

import java.io.IOException;

public interface Columnar {
    void saveTo(ColumnManager manager) throws IOException;
}
