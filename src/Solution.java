import java.io.*;
import java.util.*;



class PreProcess extends Thread {
    private final Integer Max = 5000001;
    public Integer[] GraphSet = new Integer[Max];
    public void run(){
        for(int i = 0;i < Max;++i) GraphSet[i] = i;
    }
}

class ReadRelFile extends Thread {
    private String filepath;
    public ArrayList<relPair> relPairs = new ArrayList<relPair>();
    ReadRelFile(String file){
        this.filepath = file;
    }
    public void run(){
        // 文件读取
        File file = new File(filepath);
        BufferedInputStream fis = null;
        try {
            fis = new BufferedInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader reader = null;// 用5M的缓冲读取文本文件
        try {
            reader = new BufferedReader(new InputStreamReader(fis,"utf-8"),100*1024*1024);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        String line = "";
        String[] tokens;
        while(true){
            try {
                if ((line = reader.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            // 处理
            tokens = line.split(" ");
            relPairs.add(new relPair(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[2])));
        }
    }
    //扩充数组的容量
    public static byte[] copyOf(byte[] original,int newLength){
        byte[] copy = new byte[newLength];
        System.arraycopy(original,0,copy,0,Math.min(original.length,newLength));
        return copy;
    }
}

class ReadAidFile extends Thread {
    private String filepath;
    public HashMap<Integer,String> Pairs = new HashMap<Integer, String>();
    ReadAidFile(String file){
        this.filepath = file;
    }
    public void run(){
        //文件读取
        File file = new File(filepath);
        BufferedInputStream fis = null;
        try {
            fis = new BufferedInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader reader = null;// 用缓冲读取文本文件
        try {
            reader = new BufferedReader(new InputStreamReader(fis,"utf-8"),100*1024*1024);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String[] tokens;
        String line = "";
        while(true){
            try {
                if ((line = reader.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            // 处理
            tokens = line.split(" ");
            Pairs.put(Integer.parseInt(tokens[0]),tokens[1]);
        }
    }
}


public class Solution {
    private final Integer Max = 5000001;
    private String[] tokens;
    // 用数组来存图
    private Integer[] GraphSet;
    //private Integer[] rank = new Integer[Max];
    private TreeMap<Integer, StringBuilder> Classes = new TreeMap<Integer, StringBuilder>();
    private HashMap<Integer,String> Pairs = new HashMap<Integer, String>();
    private ArrayList<relPair> relPairs = new ArrayList<relPair>();

    public void Execute(String rel,String act,String res) throws IOException, InterruptedException {
        long t1 = Calendar.getInstance().getTimeInMillis();

        //Arrays.fill(this.rank, 0);
        Vector<Thread> ths = new Vector<Thread>();
        ReadRelFile r = new ReadRelFile(rel);
        ReadAidFile a = new ReadAidFile(act);
        PreProcess p = new PreProcess();
        ths.add(r);
        ths.add(a);
        ths.add(p);
        r.start();
        a.start();
        p.start();
        Pairs = a.Pairs;
        relPairs = r.relPairs;
        GraphSet = p.GraphSet;
        for(Thread th : ths){
            th.join();
        }

        generateGraphSet();
        Classify();
        getResults(res);
        long t7 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Sum of Resume Time: " + (t7-t1) + "ms");
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

        if(x_root != y_root) if (x_root < y_root) this.GraphSet[y_root] = x_root;
        else {
            this.GraphSet[x_root] = y_root;
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
                Classes.put(GraphSet[cur], new StringBuilder());
            }
            Classes.get(GraphSet[cur]).append(Pairs.get(cur) + ',');
            //Classes.put(GraphSet[cur],s);
        }
    }

    public void getResults(String file) throws IOException {
        File f = new File(file);
        FileOutputStream fos = new FileOutputStream(f);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));
        String s = null;
        StringBuilder r = null;
        for (HashMap.Entry<Integer, StringBuilder> entry : Classes.entrySet()) {
            s = entry.getKey() + " ";
            r = entry.getValue();
            s += r.substring(0,r.length()-1)+"\r\n";
            writer.write(s);
        }
        writer.close();
        fos.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String rel = "relations.txt";
        String act = "accounts.txt";
        String res = "result.txt";
        Solution g = new Solution();
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

