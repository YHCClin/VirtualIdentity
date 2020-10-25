package FileRead;

import jdk.internal.instrumentation.Logger;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class TestRead {
    public static void main(String[] args) {
        AtomicLong counter = new AtomicLong(0);
        String file = "DataSet/accounts.txt";
        Logger log = null;
        BigFileReader.Builder builder = new BigFileReader.Builder(file, line -> log.info(line));
        BigFileReader bigFileReader = builder
                .threadPoolSize(100)
                .charset(StandardCharsets.UTF_8)
                .bufferSize(1024).build();
        bigFileReader.start();
    }

}
