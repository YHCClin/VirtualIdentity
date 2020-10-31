package FileRead;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import Solutions.*;

class relPair{
    public String A;
    public String B;
    public relPair(String a, String b){
        A = a; B = b;
    }
}

public class TestRead {

    private ArrayList<relPair> relPairs = new ArrayList<relPair>();
    public void read() throws IOException {
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

        RandomAccessFile randomAccessFile = new RandomAccessFile("DataSet/relations.txt", "rw");
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
                if (b == 10) { // 换行或回车
                    stringBuffer.flip();
                    // 这里就是一个行
                    final String line = Charset.forName("utf-8").decode(stringBuffer).toString();
                    String[] tokens = line.split(" ");
                    //boolean add = this.relPairs.add(new relPair(tokens[0], tokens[2]));
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

    public static void main(String[] args) throws IOException {
        //TestRead tr = new TestRead();
        //tr.read();

        long t = Calendar.getInstance().getTimeInMillis();
        File file = new File("DataSet/relations.txt");
        int BUFFER_SIZE = 1024;
        byte[] b = new byte[BUFFER_SIZE];
        int len = (int)file.length();
        MappedByteBuffer buff;
        try(FileChannel channel = new FileInputStream(file).getChannel()) {
            buff = channel.map(FileChannel.MapMode.READ_ONLY,0,channel.size());
            for(int offset = 0;offset < len;offset += BUFFER_SIZE){
                if(len - offset > BUFFER_SIZE){
                    buff.get(b);
                } else {
                    buff.get(new byte[len - offset]);
                }

                //final String line = StandardCharsets.UTF_8.decode(buff).toString();
                //System.out.println(line);
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        long t1 = Calendar.getInstance().getTimeInMillis();
        System.out.println("time:  "+(t1-t)+"ms");
    }

    private static ByteBuffer reAllocate(ByteBuffer stringBuffer) {
        final int capacity = stringBuffer.capacity();
        byte[] newBuffer = new byte[capacity * 2];
        System.arraycopy(stringBuffer.array(), 0, newBuffer, 0, capacity);
        return (ByteBuffer) ByteBuffer.wrap(newBuffer).position(capacity);
    }
}

class Relations{

}