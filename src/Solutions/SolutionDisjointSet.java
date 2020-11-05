package Solutions;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TreeMap;


public class SolutionDisjointSet {

    private final Integer Max = 5000001;
    private String[] tokens;
    // 用数组来存图
    private Integer[] GraphSet = new Integer[Max];
    //private Integer[] rank = new Integer[Max];
    private TreeMap<Integer, String> Classes = new TreeMap<Integer, String>();
    private HashMap<Integer,String> Pairs = new HashMap<Integer, String>();
    private ArrayList<relPair> relPairs = new ArrayList<relPair>();
    String[] relFile;
    String[] idcFile;

    public void Execute(String rel,String act,String res) throws IOException {
        System.out.println("============== The Method of Disjoint Set ============");
        long t1 = Calendar.getInstance().getTimeInMillis();
        for(int i = 0;i < Max;++i) GraphSet[i] = i;
        //Arrays.fill(this.rank, 0);
        System.out.println("Reading Files...");
        //getRelFromFile(rel);
        getIdAcPair(act);
        ReadRelations(rel);
        long t2 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Done! Resume Time: " + (t2-t1) + "ms");
        System.out.println("Generate GraphSet...");
        generateGraphSet();
        long t3 = Calendar.getInstance().getTimeInMillis();
        long t4 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Done! Resume Time: " + (t4-t2) + "ms");

        System.out.println("Excute Classifying...");
        Classify();
        long t5 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Done! Resume Time: " + (t5-t4) + "ms");

        System.out.println("Generating Results File...");
        getResults(res);
        long t6 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Done! Resume Time: " + (t6-t5) + "ms");

        System.out.println("All works finished!");

        long t7 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Sum of Resume Time: " + (t7-t1) + "ms");

        long t = (t2 - t1) + (t4 - t3) + (t6 - t5);
        System.out.println("IO: " + t + "ms");
        System.out.println("AL: " + ((t7-t1) - t) + "ms");
        System.out.println("============== The Method of Disjoint Set Done. ============");
    }


    public void ReadRelations(String file) throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(file,"r");
        int size =(int)memoryMappedFile.length();
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY,0,size);
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
                    break;
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
                String line = new String(buf,"ascii");
                tokens = line.split(" ");
                relPairs.add(new relPair(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[2])));
                flag = false;
                buf = null;
                count = extra;
                buf = new byte[count];
                j =0;
            }
        }
        //处理最后一次读取
        if(j>0){
            String line = new String(buf,"ascii");
            System.out.println(line);
        }
        memoryMappedFile.close();
    }
    public void ReadAccounts(String file) throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(file,"r");
        int size =(int)memoryMappedFile.length();
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY,0,size);
        //要根据文件行的平均字节大小来赋值
        final int extra = 30;
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
                    break;
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
                String line = new String(buf,"ascii");
                tokens = line.split(" ");
                Pairs.put(Integer.parseInt(tokens[0]),tokens[1]);
                flag = false;
                buf = null;
                count = extra;
                buf = new byte[count];
                j =0;
            }
        }
        //处理最后一次读取
        if(j>0){
            String line = new String(buf,"ascii");
            System.out.println(line);
        }
        memoryMappedFile.close();
    }
    //扩充数组的容量
    public static byte[] copyOf(byte[] original,int newLength){
        byte[] copy = new byte[newLength];
        System.arraycopy(original,0,copy,0,Math.min(original.length,newLength));
        return copy;
    }

    public void getIdAcPair(String filepath) throws IOException {

        File file = new File(filepath);
        BufferedReader reader = null;
        String[] tokens;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 0;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                tokens = tempString.split(" ");
                Pairs.put(line,tokens[1]);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public Integer getRoot(Integer x){
        Integer x_root = x;
        while(!this.GraphSet[x_root].equals(x_root)) {
            x_root = this.GraphSet[x_root];
        }
        return x_root;
    }

    public void Union_verts(Integer x, Integer y) {
        int x_root = getRoot(x);
        int y_root = getRoot(y);
        /*
            if(this.rank[x_root] > this.rank[y_root])
                this.GraphSet[y_root] = x_root;
            else if(this.rank[x_root] < this.rank[y_root])
                this.GraphSet[x_root] = y_root;
            else {
                this.GraphSet[x_root] = y_root;
                this.rank[y_root]++;
            }
             */
        // 由于要让最小编号作为根，故先不考虑优化，而且树高不是很高因为每个簇大小最多就11,12的样子
        // 这样也能保证每一个簇里面的账号都是按id排序的
        if(x_root != y_root) if (x_root < y_root) this.GraphSet[y_root] = x_root;
        else {
            this.GraphSet[x_root] = y_root;
        }
    }

    public void getRelFromFile(String file) {

        BufferedReader br;
        try {
            File f = new File(file);
            br = new BufferedReader(new FileReader(f));
            String s;
            String[] tokens;
            int A,B;
            while((s = br.readLine()) != null){
                tokens = s.split(" ");
                A = Integer.parseInt(tokens[0]);
                B = Integer.parseInt(tokens[2]);
                relPairs.add(new relPair(A,B));
                Union_verts(A,B);
            }
            br.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public void generateGraphSet() {
        for (relPair rp : relPairs) {
            Union_verts(rp.A,rp.B);
        }
    }

    public void UnionToRoot(){
        for(Integer v = 0;v < Max;++v){
            if(!GraphSet[v].equals(v)){
                GraphSet[v] = getRoot(v);
            }
        }
    }

    public void Classify(){
        UnionToRoot();
        for(int cur = 0;cur < Max;++cur) {
            if (!Classes.containsKey(GraphSet[cur])) {
                Classes.put(GraphSet[cur], "");
            }
            String s = Classes.get(GraphSet[cur]).concat(Pairs.get(cur) + ',');
            Classes.put(GraphSet[cur],s);
        }
    }

    public void getResults(String file) throws IOException {
        File f = new File(file);
        FileOutputStream fos = new FileOutputStream(f);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));
        String s = null;
        String r = null;
        for (HashMap.Entry<Integer, String> entry : Classes.entrySet()) {
            s = entry.getKey() + " ";
            r = entry.getValue();
            s += r.substring(0,r.length()-1)+"\r\n";
            writer.write(s);
        }
        writer.close();
        fos.close();
    }

    public static void main(String[] args) throws IOException {
        String rel = "DataSet/relations.txt";
        String act = "DataSet/accounts.txt";
        String res = "DataSet/result.txt";
        SolutionDisjointSet g = new SolutionDisjointSet();
        g.Execute(rel,act,res);
    }
}

class relPair{
    public Integer A;
    public Integer B;
    public relPair(Integer a, Integer b){
        A = a; B = b;
    }
}
