package FileRead;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/*
    实现了按行读取
 */

public class testMemoryMappedFile {


    public ArrayList<relPair> relPairs = new ArrayList<relPair>();
    private String[] tokens;

    public void Read(String file) throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(file,"r");
        int size =(int)memoryMappedFile.length();
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY,0,size);
        long start = System.currentTimeMillis();
        //要根据文件行的平均字节大小来赋值
        final int extra = 55;
        int count = extra;
        byte[] buf = new byte[count];
        int j=0;
        char ch ='\0';
        boolean flag = false;
        while(out.remaining()>0){
            byte by = out.get();
            ch =(char)by;
            switch(ch){
                case '\n':
                    flag = true;
                    break;/*
                case '\r':
                    flag = true;
                    break;
                   */
                default:
                    buf[j] = by;
                    break;
            }
            j++;
            //读取的字符超过了buf 数组的大小，需要动态扩容
            if(flag ==false && j>=count){
                count = count + extra;
                buf = copyOf(buf,count);
            }
            if(flag==true){
                //这里的编码要看文件实际的编码
                String line = new String(buf,"utf-8");
                //System.out.println(line);
                tokens = line.split(" ");
                //relPairs.add(new relPair(tokens[0],tokens[2]));
                System.out.println(tokens[0] + " "+ tokens[2]);
                flag = false;
                buf = null;
                count = extra;
                buf = new byte[count];
                j =0;
            }
        }
        //处理最后一次读取
        if(j>0){
            String line = new String(buf,"utf-8");
            System.out.println(line);
        }

        long end = System.currentTimeMillis();
        System.out.println("耗时:"+(end-start));
        memoryMappedFile.close();

    }
    //扩充数组的容量
    public static byte[] copyOf(byte[] original,int newLength){
        byte[] copy = new byte[newLength];
        System.arraycopy(original,0,copy,0,Math.min(original.length,newLength));
        return copy;
    }

    public static void main(String[] args) throws IOException {
        new testMemoryMappedFile().Read("DataSet/relations.txt");
    }
}
