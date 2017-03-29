package qp.sorter;

import java.io.*;

public abstract class DataWriterFactory<T>
{
    public abstract DataWriter<T> constructWriter(OutputStream out) throws IOException;
}
