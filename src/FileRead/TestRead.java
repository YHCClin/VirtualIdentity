package FileRead;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicLong;

public class TestRead {
    public static void main(String[] args) throws IOException {
        /*
        AtomicLong counter = new AtomicLong(0);
        String file = "DataSet/accounts.txt";
        Logger log = null;
        BigFileReader.Builder builder = new BigFileReader.Builder(file, line -> log.info(line));
        BigFileReader bigFileReader = builder
                .threadPoolSize(10)
                .charset(StandardCharsets.UTF_8)
                .bufferSize(1024).build();
        bigFileReader.start();

         */
        long t = Calendar.getInstance().getTimeInMillis();
        RandomAccessFile randomAccessFile = new RandomAccessFile("DataSet/accounts.txt", "rw");
        FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
        int bytesRead = channel.read(buffer);
        ByteBuffer stringBuffer = ByteBuffer.allocate(20);
        while (bytesRead != -1) {
            //System.out.println("读取字节数：" + bytesRead);
            //之前是写buffer，现在要读buffer
            buffer.flip();// 切换模式，写->读
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if (b == 10 || b == 13) { // 换行或回车
                    stringBuffer.flip();
                    // 这里就是一个行
                    final String line = Charset.forName("utf-8").decode(stringBuffer).toString();
                    //String[] tokens = line.split(" ");
                    //System.out.println(line);// 解码已经读到的一行所对应的字节
                    stringBuffer.clear();
                } else {
                    if (stringBuffer.hasRemaining())
                        stringBuffer.put(b);
                    else { // 空间不够扩容
                        stringBuffer = reAllocate(stringBuffer);
                        stringBuffer.put(b);
                    }
                }
            }
            buffer.clear();// 清空,position位置为0，limit=capacity
            //  继续往buffer中写
            bytesRead = channel.read(buffer);
        }
        randomAccessFile.close();
        long t1 = Calendar.getInstance().getTimeInMillis();
        System.out.println("time: "+(t1-t));
    }

    private static ByteBuffer reAllocate(ByteBuffer stringBuffer) {
        final int capacity = stringBuffer.capacity();
        byte[] newBuffer = new byte[capacity * 2];
        System.arraycopy(stringBuffer.array(), 0, newBuffer, 0, capacity);
        return (ByteBuffer) ByteBuffer.wrap(newBuffer).position(capacity);
    }
}
