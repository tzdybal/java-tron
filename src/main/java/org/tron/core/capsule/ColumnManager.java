package org.tron.core.capsule;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ColumnManager {
    private String prefix;
    private Map<String, BufferedOutputStream> columns;

    public ColumnManager(String prefix) {
        this.prefix = prefix;
        columns = new HashMap<>();
    }

    public void addRow(String column, byte[] data) throws IOException {
        BufferedOutputStream stream = columns.get(column);
        if (stream == null) {
            stream = initColumn(column);
        }
        stream.write(data);
    }

    public void flush() throws IOException {
        IOException lastException = null;
        for (Map.Entry<String, BufferedOutputStream> entry : columns.entrySet()) {
            BufferedOutputStream stream = entry.getValue();
            try {
                stream.flush();
            } catch (IOException ex) {
                lastException = ex;
            }
        }
        if (lastException != null) throw lastException;
    }

    public void close() throws IOException {
        IOException lastException = null;
        for (Map.Entry<String, BufferedOutputStream> entry : columns.entrySet()) {
            BufferedOutputStream stream = entry.getValue();
            try {
                stream.close();
            } catch (IOException ex) {
                lastException = ex;
            }
        }
        if (lastException != null) throw lastException;
    }

    private BufferedOutputStream initColumn(String column) throws IOException {
        File file = new File(prefix + File.separator + column + ".column");
        new FileWriter(file).close(); // create if possible
        BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(file), 100 * 1024 * 1024);
        columns.put(column, stream);
        return stream;
    }

}
