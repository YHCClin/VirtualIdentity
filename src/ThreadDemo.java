import javafx.util.Pair;
import jdk.management.resource.internal.inst.InitInstrumentation;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ThreadDemo extends Thread{
    //文件名
    private String filepath;
    private File file;
    // 不同的文件有不同的处理, Mod=0表示处理rel文件, Mod=1 表示处理act文件
    private Boolean Mod;
    //读取当前cpu个数，决定线程池的大小
    private final static int poolSize=Runtime.getRuntime().availableProcessors();
    //任务队列(阻塞队列)，生产者放数据，消费者读数据
    private  ArrayBlockingQueue<List<String>> queue=new ArrayBlockingQueue<>(poolSize);
    //存放转换后的数据,关系对 以及 id:act
    public List<pair> relList=new ArrayList<>();
    public List<id_act> idActsList = new ArrayList<>();
    //private List<List<String>> arrayList=new ArrayList<>();
    // 存放读出的pair对
    public HashMap<Integer,String> Pairs = new HashMap<Integer, String>();
    //线程池
    private ExecutorService executorService= Executors.newFixedThreadPool(poolSize);
    //定义文件片大小
    private int lineNumber;

    /**
     * Constructor
     * @param filepath
     */
    public ThreadDemo(String filepath, Boolean Mod){
        this.file = new File(filepath);
        this.filepath = filepath;
        this.Mod = Mod;
    }
    /**
     * 文件分片
     *
     */
    private void splitFile(){
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            int count=0;
            while(br.readLine()!=null){
                count++;
            }
            br.close();
            lineNumber=count/poolSize;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * (生产者)将文件分片后把每一部分的数据封装到list集合中,入队
     * @param file
     */
    private void putTask(File file) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        try {
            String line;
            //入队操作，把每一个线程处理的数据封装到list集合中入队
            List<String> lines = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                lines.add(line);
                if (lines.size() >= lineNumber) {
                    queue.put(lines);
                    lines = new ArrayList<>();
                }
            }
            //放入最后的剩余部分
            if (lines.size()!=0){
                queue.put(lines);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            br.close();
            System.out.println("数据入队完成");
        }
    }

    /**
     * (消费者)处理数据(关系对)
     * 从阻塞队列中读取分片内容
     */
    private void tranfRelData() throws InterruptedException {
        AtomicInteger startIndex =new AtomicInteger(1);
        while (true){
            //从队列中取数据，等待1秒
            List<String> lines = queue.poll(1, TimeUnit.SECONDS);
            if(lines==null){
                //当从队列中取不出数据时，结束。
                break;
            }
            executorService.execute(() ->{
                int index=startIndex.getAndAdd(lines.size());
                System.out.println(Thread.currentThread().getName()+"====第"+index+"-"+(index+lines.size()-1)+"条数据");
                //处理数据
                List<pair> stringList = lines.stream().map(x -> {
                    String[] tokens;
                    tokens = x.split(" ");
                    return new pair(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[2]));
                }).collect(Collectors.toList());
                //List<String> stringList = lines.stream().map(x -> x + "helloworld\r\n").collect(Collectors.toList());
                relList.addAll(stringList);
            });
        }
        //当前任务执行完后，释放线程资源。
        executorService.shutdown();
        while(true){
            if(executorService.isTerminated()){
                break;
            }
        }
    }

    /**
     * (消费者)处理数据(Id Acount 对)
     * 从阻塞队列中读取分片内容
     */
    private void tranfIdActData() throws InterruptedException {
        AtomicInteger startIndex =new AtomicInteger(1);
        while (true){
            //从队列中取数据，等待1秒
            List<String> lines = queue.poll(1, TimeUnit.SECONDS);
            if(lines==null){
                //当从队列中取不出数据时，结束。
                break;
            }
            executorService.execute(() ->{
                int index=startIndex.getAndAdd(lines.size());
                System.out.println(Thread.currentThread().getName()+"====第"+index+"-"+(index+lines.size()-1)+"条数据");
                //处理数据
                List<id_act> stringList = lines.stream().map(x -> {
                    String[] tokens;
                    tokens = x.split(" ");
                    return new id_act(Integer.parseInt(tokens[0]),tokens[1]);
                }).collect(Collectors.toList());
                //List<String> stringList = lines.stream().map(x -> x + "helloworld\r\n").collect(Collectors.toList());
                idActsList.addAll(stringList);
            });
        }
        //当前任务执行完后，释放线程资源。
        executorService.shutdown();
        while(true){
            if(executorService.isTerminated()){
                break;
            }
        }
    }

    /**
     * 处理后的数据写入文件
     */

    private void writeToFile(){
        try {
            String outputfile = null;
            if(!Mod) outputfile = "ProcessedRel.txt";
            else outputfile = "ProcessedIdAct.txt";
            BufferedWriter bw =new BufferedWriter(new FileWriter(outputfile,false));
            StringBuilder str =new StringBuilder();
//          arrayList.stream().flatMap(Collection::stream).forEach(str::append);
            //List<String> collect = arrayList.stream().flatMap(Collection::stream).collect(Collectors.toList());
            System.out.println();
            if(!Mod)
                for(pair p:relList){
                    String line = Integer.toString(p.a) + " " + Integer.toString(p.b) + "\r\n";
                    str.append(line);
                }
            else
                for(id_act ia:idActsList){
                    String line = Integer.toString(ia.id) + " " + ia.act + "\r\n";
                    str.append(line);
                }
            bw.write(str.toString());
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();    //获取开始时间
        this.splitFile();
        this.executorService.execute(()-> {
            try {
                this.putTask(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            if(!Mod)
                this.tranfRelData();
            else this.tranfIdActData();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //this.writeToFile();
        long endTime = System.currentTimeMillis();
        System.out.println("本次耗时："+(endTime-startTime));
    }

    public static void main(String[] args) throws InterruptedException {
        ThreadDemo tact = new ThreadDemo("accounts.txt",true);
        ThreadDemo trel = new ThreadDemo("relations.txt",false);

        //tact.start();
        //tact.join();

        trel.start();
        trel.join();
        System.out.println(tact.idActsList.size());
        System.out.println(tact.relList.size());
        System.out.println(trel.idActsList.size());
        System.out.println(trel.relList.size());
    }
}

class pair {
    public int a,b;
    public pair(int a, int b){
        this.a = a;
        this.b = b;
    }
}

class id_act {
    public int id;
    public String act;
    public id_act(int id, String act){
        this.id = id;
        this.act = act;
    }
}