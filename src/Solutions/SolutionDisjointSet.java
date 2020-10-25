package Solutions;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TreeMap;


public class SolutionDisjointSet {

    private final Integer Max = 5000001;
    // 用数组来存图
    private Integer[] GraphSet = new Integer[Max];
    //private Integer[] rank = new Integer[Max];
    private TreeMap<Integer, ArrayList<Integer>> Classes = new TreeMap<Integer, ArrayList<Integer>>();
    private HashMap<Integer,String> Pairs = new HashMap<Integer, String>();
    private ArrayList<relPair> relPairs = new ArrayList<relPair>();


    public void Execute(String rel,String act,String res) throws IOException {
        System.out.println("============== The Method of Disjoint Set ============");
        long t1 = Calendar.getInstance().getTimeInMillis();
        for(int i = 0;i < Max;++i) GraphSet[i] = i;
        //Arrays.fill(this.rank, 0);
        System.out.println("Reading Relations File...");
        getRelFromFile(rel);
        long t2 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Done! Resume Time: " + (t2-t1) + "ms");
        //System.out.println("Generate GraphSet...");
        //generateGraphSet();
        long t3 = Calendar.getInstance().getTimeInMillis();
        //System.out.println("Done! Resume Time: " + (t3-t2) + "ms");

        System.out.println("Reading Account File...");
        getIdAcPair(act);
        long t4 = Calendar.getInstance().getTimeInMillis();
        System.out.println("Done! Resume Time: " + (t4-t3) + "ms");

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

    public void getIdAcPair(String file) throws IOException {
        //long t = Calendar.getInstance().getTimeInMillis();
        /*
        Files.lines(Paths.get(file)).forEach(line -> {
            String[] split = line.split(" ");
            Integer id = Integer.parseInt(split[0]);
            Pairs.put(id,split[1]);
        });

         */

        BufferedReader br;
        File f = new File(file);
        br = new BufferedReader(new FileReader(f));
        String s;
        String[] tokens;
        int id;
        String cnt;
        while((s = br.readLine()) != null){
            tokens = s.split(" ");
            id = Integer.parseInt(tokens[0]);
            cnt = tokens[1];
            Pairs.put(id,cnt);
        }
        br.close();


        //long t1 = Calendar.getInstance().getTimeInMillis();
        //System.out.println("Time: "+(t1-t)+"ms");
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
        //long t = Calendar.getInstance().getTimeInMillis();
        /*
        Files.lines(Paths.get(file)).forEach(line -> {
            String[] split = line.split(" ");
            Integer A = Integer.parseInt(split[0]);
            Integer B = Integer.parseInt(split[2]);
            //relPairs.add(new relPair(A,B));
            Union_verts(A, B);
        });
         */

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
                //relPairs.add(new relPair(A,B));
                Union_verts(A,B);
            }
            br.close();
        } catch (IOException e){
            e.printStackTrace();
        }

        //long t1 = Calendar.getInstance().getTimeInMillis();
        //System.out.println("Time: "+(t1-t)+"ms");
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
        //long t = Calendar.getInstance().getTimeInMillis();
        UnionToRoot();
        for(int cur = 0;cur < Max;++cur) {
            if (!Classes.containsKey(GraphSet[cur])) {
                Classes.put(GraphSet[cur], new ArrayList<>());
            }
            Classes.get(GraphSet[cur]).add(cur);
        }
        //long t1 = Calendar.getInstance().getTimeInMillis();
        //System.out.println("Time: "+(t1-t)+"ms");
    }

    public void getResults(String file) throws IOException {
        //long t = Calendar.getInstance().getTimeInMillis();
        //FileOutputStream fos = new FileOutputStream(file,true);
        //FileChannel channel = fos.getChannel();
        //StringBuffer content = new StringBuffer();
        File f = new File(file);
        FileWriter rs = new FileWriter(f);
        BufferedWriter buff = new BufferedWriter(rs);
        StringBuffer s = new StringBuffer();
        for (HashMap.Entry<Integer, ArrayList<Integer>> entry : Classes.entrySet()) {
            s.append(entry.getKey() + "");
            for(int i = 0;i < entry.getValue().size();i++){
                s.append((i==0 ? " " : ",") + Pairs.get(entry.getValue().get(i)));
            }
            s.append("\r\n");
            
            //buff.write(s + "\r\n");
        }
        buff.write(String.valueOf(s));
        buff.close();
        //long t1 = Calendar.getInstance().getTimeInMillis();
        //System.out.println("Time: "+(t1-t)+"ms");
    }

    public static void main(String[] args) throws IOException {
        String rel = "DataSet/relations.txt";
        String act = "DataSet/accounts.txt";
        String res = "DataSet/results.txt";
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
