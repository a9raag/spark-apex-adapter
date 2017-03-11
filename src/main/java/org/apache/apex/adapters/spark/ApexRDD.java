package org.apache.apex.adapters.spark;

import alluxio.exception.AlluxioException;
import com.datatorrent.api.Context;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.stram.client.StramAppLauncher;
import org.apache.apex.adapters.spark.apexscala.ApexPartition;
import org.apache.apex.adapters.spark.apexscala.ScalaApexRDD;
import org.apache.apex.adapters.spark.io.ReadFromFS;
import org.apache.apex.adapters.spark.operators.*;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.*;
import scala.collection.Iterator;
import scala.collection.Map;
import scala.math.Ordering;
import scala.reflect.ClassTag;

import java.io.*;
import java.io.Serializable;
import java.lang.Long;
import java.util.*;

import static com.datatorrent.api.Context.OperatorContext.PARTITIONER;
import static org.apache.apex.adapters.spark.io.ReadFromFS.successFileExists;
import static org.apache.apex.adapters.spark.io.WriteToFS.deleteSUCCESSFile;

public class ApexRDD<T> extends ScalaApexRDD<T> implements Serializable {
    private static final long serialVersionUID = -3545979419189338756L;
    public static ApexContext context;
    public static ApexContext _sc;
    private static PairRDDFunctions temp;
    public BaseOperatorSerializable currentOperator;
    public OperatorType currentOperatorType;
    public DefaultOutputPortSerializable currentOutputPort;
    public DefaultOutputPortSerializable controlOutput;
    public SerializableDAG dag;
    public ApexRDDPartitioner apexRDDPartitioner = new ApexRDDPartitioner();
    public Partition[] partitions_=getPartitions();
    protected Option<Partitioner> partitioner = (Option<Partitioner>) new ApexRDDOptionPartitioner();
    Logger log = LoggerFactory.getLogger(ApexRDD.class);
    boolean launchOnCluster=true;


    public ApexRDD(RDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
        this.dag=((ApexRDD<T>)rdd).dag;

    }

    public ApexRDD(ApexContext ac) {
        super(ac.emptyRDD((ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class)), (ClassTag<T>) scala.reflect.ClassManifestFactory.fromClass(Object.class));
//        super.setSparkContext(context);
        dag = new SerializableDAG();
        context=ac;
        _sc=ac;
    }

    @Override
    public SparkContext sparkContext() {
        return context;
    }

    @Override
    public Option<Partitioner> partitioner() {
        return new ApexRDDOptionPartitioner();
    }

    @Override
    public SparkContext context() {
        return context;
    }

    public SerializableDAG getDag() {
        return this.dag;
    }

    public DefaultOutputPortSerializable getCurrentOutputPort(SerializableDAG cloneDag){

        try {
            log.debug("Last operator in the Dag {}",dag.getLastOperatorName());
            BaseOperatorSerializable currentOperator = (BaseOperatorSerializable) cloneDag.getOperatorMeta(cloneDag.getLastOperatorName()).getOperator();
            return currentOperator.getOutputPort();
        } catch (Exception e) {
            //System.out.println("Operator "+cloneDag.getLastOperatorName()+" Doesn't exist in the dag");
            e.printStackTrace();
        }
        return currentOperator.getOutputPort();
    }
    public DefaultInputPortSerializable getFirstInputPort(SerializableDAG cloneDag){
        BaseInputOperatorSerializable currentOperator= (BaseInputOperatorSerializable) cloneDag.getOperatorMeta(cloneDag.getFirstOperatorName()).getOperator();
        return currentOperator.getInputPort();
    }
    public DefaultOutputPortSerializable getControlOutput(SerializableDAG cloneDag){
        if(getProperty("fs").equals("hdfs")) {
            InputSplitOperator currentInputSplitOperator = (InputSplitOperator) cloneDag.getOperatorMeta(cloneDag.getFirstOperatorName()).getOperator();
            return currentInputSplitOperator.getControlOut();
        }
        else {
            BaseInputOperatorSerializable currentOperator= (BaseInputOperatorSerializable) cloneDag.getOperatorMeta(cloneDag.getFirstOperatorName()).getOperator();
            return currentOperator.getControlOut();
        }
    }
    public InputSplitOperator<T> getInputSplitOperator(SerializableDAG cloneDag){
        return (InputSplitOperator) cloneDag.getOperatorMeta(cloneDag.getFirstOperatorName()).getOperator();
    }
    public void enableParallelPartition(BaseOperatorSerializable currentOperator, SerializableDAG cloneDag){
        int minPartitions = getInputSplitOperator(cloneDag).minPartitions;
        if (minPartitions > 1) {
            cloneDag.setAttribute(currentOperator, PARTITIONER, new StatelessPartitioner<BaseOperatorSerializable>(minPartitions));
            cloneDag.setInputPortAttribute(currentOperator.getInputPort(), Context.PortContext.PARTITION_PARALLEL, true);
        }
    }

    public <U> RDD<U> map(Function <T,U> f){

        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperatorFunction m1 = cloneDag.addOperator(System.currentTimeMillis()+ " MapFunction " , new MapOperatorFunction());
        m1.f=context.clean(f,true);

//        ScalaApexRDD$.MODULE$.test((ScalaApexRDD<Tuple2<Object, Object>>) this, (ClassTag<Object>) evidence$3,null,null);
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream Function", currentOutputPort, m1.input);
       // cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        enableParallelPartition(m1,cloneDag);
        ApexRDD<U> temp= (ApexRDD<U>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
    }




    @Override
    public T[] collect() {
        SerializableDAG cloneDag= (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CollectOperator collectOperator =cloneDag.addOperator(System.currentTimeMillis()+" Collect Operator",CollectOperator.class);

//        cloneDag.setInputPortAttribute(collectOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,collectOperator.input);
        FileWriterOperator fileWriter = cloneDag.addOperator(System.currentTimeMillis()+" File Writer Operator",FileWriterOperator.class);
        fileWriter.setAbsoluteFilePath("collected");
        cloneDag.addStream(System.currentTimeMillis()+" FileWriter Stream",collectOperator.output,fileWriter.input);
        try {
            launch(cloneDag,3000,"collect",launchOnCluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(!successFileExists()) {
            log.info("Waiting for the _SUCCESS file");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ArrayList<T> collected= (ArrayList<T>) ReadFromFS.read("collected");
        deleteSUCCESSFile();
        return (T[]) collected.toArray();
    }

    @Override
    public T[] take(int num) {

        SerializableDAG cloneDag= (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        TakeOperator takeOperator =cloneDag.addOperator(System.currentTimeMillis()+" Take Operator",TakeOperator.class);
        takeOperator.count=num;
        cloneDag.addStream(System.currentTimeMillis()+" Collect Stream",currentOutputPort,takeOperator.input);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("selectedData");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", takeOperator.output, writer.input);
        try {
            launch(cloneDag,3000,"take",launchOnCluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<T> array = (ArrayList<T>) ReadFromFS.read("selectedData");
        deleteSUCCESSFile();
//        T[] arrayT= (T[]) Arrays.copyOf(array.toArray(),array.toArray().length,Integer[].class);
        return (T[]) array.toArray();
    }

    @Override
    public <U> RDD<U> map(Function1<T, U> f, ClassTag<U> evidence$3) {

        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        MapOperator m1 = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new MapOperator());
        m1.f= f;
        cloneDag.addStream( System.currentTimeMillis()+ " MapStream ", currentOutputPort, m1.input);
        enableParallelPartition(m1,cloneDag);
        ApexRDD<U> temp= (ApexRDD<U>) createClone(cloneDag);
        return temp;
    }

    @Override
    public RDD<T> filter(Function1<T, Object> f) {
        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        FilterOperator filterOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Filter", FilterOperator.class);
        filterOperator.f = context.clean(f,true);
        cloneDag.addStream(System.currentTimeMillis()+ " FilterStream " + 1, currentOutputPort, filterOperator.input);
        enableParallelPartition(filterOperator,cloneDag);
        return createClone(cloneDag);
    }

    @Override
    public RDD<T> persist(StorageLevel newLevel) {
        return this;
    }

    @Override
    public RDD<T> unpersist(boolean blocking) {
        return this;
    }

    public RDD<T>[] randomSplit(double[] weights){
        return randomSplit(weights, new Random().nextLong());
    }

    @Override
    public <U> RDD<U> mapPartitions(Function1<Iterator<T>, Iterator<U>> f, boolean preservesPartitioning, ClassTag<U> evidence$6) {

        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput=getControlOutput(cloneDag);
        MapPartitionOperator mapPartitionOperator= cloneDag.addOperator(System.currentTimeMillis()+ " MapPartition " , new MapPartitionOperator());
        mapPartitionOperator.f = f;
        cloneDag.addStream( System.currentTimeMillis()+ " MapPartitionStream ", currentOutputPort, mapPartitionOperator.input);
       // cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        enableParallelPartition(mapPartitionOperator,cloneDag);
        ApexRDD<U> temp= (ApexRDD<U>) createClone(cloneDag);
        return temp;
    }

    @Override
    public T reduce(Function2<T, T, T> f) {
        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        //DefaultInputPortSerializable firstInputPort = getFirstInputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        ReduceOperator reduceOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Reduce " , new ReduceOperator());
       // cloneDag.setInputPortAttribute(reduceOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        reduceOperator.f = context.clean(f,true);
//        reduceOperator.f1=f;
        Assert.assertTrue(currentOutputPort != null);
        cloneDag.addStream(System.currentTimeMillis()+" Reduce Input Stream", currentOutputPort, reduceOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, reduceOperator.controlDone);

        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
        //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("reducedValue");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", reduceOperator.output, writer.input);

        try {
            launch(cloneDag,3000,"reduce",launchOnCluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(!successFileExists()) {
            log.info("Waiting for the _SUCCESS file");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        T reducedValue= (T) ReadFromFS.read("reducedValue");
        deleteSUCCESSFile();
        return reducedValue;
    }

    @Override
    public Map<T, Object> countByValue(Ordering<T> ord) {
        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        CountByValueOperator countByVlaueOperator = cloneDag.addOperator(System.currentTimeMillis() + "" +
                " CountByValueOperator", CountByValueOperator.class);
        controlOutput = getControlOutput(cloneDag);
        cloneDag.addStream(System.currentTimeMillis() + " CountValue Stream", currentOutputPort, countByVlaueOperator.getInputPort());
//        getBaseInputOperator(cloneDag).appName = countByValueApp;
        //cloneDag.setInputPortAttribute(countByVlaueOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());

        cloneDag.addStream(System.currentTimeMillis() + " ControlDone Stream", controlOutput, countByVlaueOperator.controlDone);
        FileWriterOperator fileWriterOperator = cloneDag.addOperator(System.currentTimeMillis() + "WriteMap ", new FileWriterOperator());
        fileWriterOperator.setAbsoluteFilePath("countByValueOutput");
//        fileWriterOperator.appName = countByValueApp;
        cloneDag.addStream(System.currentTimeMillis() +  " MapWrite", countByVlaueOperator.output, fileWriterOperator.input);
        try {
            launch(cloneDag,3000,"countByValue",launchOnCluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(!successFileExists()) {
            log.info("Waiting for the _SUCCESS file");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        HashMap<T, Object> map = (HashMap<T, Object>) ReadFromFS.read("countByValueOutput");
        deleteSUCCESSFile();
        return scala.collection.JavaConversions.asScalaMap(map);
    }

    public void foreach(VoidFunction<T> voidFunction) {
        SerializableDAG cloneDag= (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        ForEachOperator foreach = cloneDag.addOperator(System.currentTimeMillis()+" ForEachOperator",new ForEachOperator());
//        getBaseInputOperator(cloneDag).appName=forEachApp;

        foreach.voidFunction = voidFunction;
        cloneDag.addStream(System.currentTimeMillis()+" ForEachStream", currentOutputPort, foreach.input);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", new FileWriterOperator());

        writer.setAbsoluteFilePath("chiFilteredData");

        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", foreach.output, writer.input);

        try {
            launch(cloneDag,3000,"forEach",launchOnCluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(!successFileExists()) {
            log.info("Waiting for the _SUCCESS file");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("All dags launched Successfully...");

    }

    @Override
    public Iterator<T> compute(Partition arg0, TaskContext arg1) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public <U> U aggregate(U zeroValue, Function2<U, T, U> seqOp, Function2<U, U, U> combOp, ClassTag<U> evidence$29) {
        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        AggregateOperator aggregateOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Aggregate Operator " , new AggregateOperator());
        aggregateOperator.zeroValue = zeroValue;
        aggregateOperator.seqFunction = seqOp;
        aggregateOperator.combFunction = combOp;
        Assert.assertTrue(currentOutputPort != null);
        cloneDag.addStream(System.currentTimeMillis()+" Aggregator Operator Stream", currentOutputPort, aggregateOperator.input);
        FileWriterOperator fileWriterOperator = cloneDag.addOperator(System.currentTimeMillis()+"FileWrite Operator from Aggregate",new FileWriterOperator());
        fileWriterOperator.setAbsoluteFilePath("/tmp/aggregateOutput");
        cloneDag.addStream(System.currentTimeMillis()+"File Operator Stream",aggregateOperator.output,fileWriterOperator.input);
        cloneDag.validate();
        log.debug("DAG successfully validated");

        try {
            launch(cloneDag,3000,"collect",launchOnCluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (U) aggregateOperator.aggregateValue;
    }

    @Override
    public Partition[] getPartitions() {
        // TODO Auto-generated method stub
        ApexPartition[] partitions = new ApexPartition[apexRDDPartitioner.numPartitions()];
        ApexPartition partition = new ApexPartition();
        partitions[0]=partition;
        return partitions;
    }

    @Override
    public long count() {
        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentCountOutputPort = getCurrentOutputPort(cloneDag);
        controlOutput= getControlOutput(cloneDag);
        CountOperator countOperator = cloneDag.addOperator(System.currentTimeMillis()+ " CountOperator " , CountOperator.class);
       // cloneDag.setInputPortAttribute(countOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" Count Input Stream", currentCountOutputPort, countOperator.input);
        cloneDag.addStream(System.currentTimeMillis()+" ControlDone Stream", controlOutput, countOperator.controlDone);
        FileWriterOperator writer = cloneDag.addOperator( System.currentTimeMillis()+" FileWriter", FileWriterOperator.class);
       // cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        writer.setAbsoluteFilePath("count");
        cloneDag.addStream(System.currentTimeMillis()+"FileWriterStream", countOperator.output, writer.input);
        try {
            launch(cloneDag,3000,"count",launchOnCluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(!successFileExists()) {
            log.info("Waiting for the _SUCCESS file");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Long count= (Long) ReadFromFS.read("count");
        if(count==null)
            return 0L;
        deleteSUCCESSFile();
        return count;
    }

    @Override
    public ApexRDD<T>[] randomSplit(double[] weights, long seed){
        long count =this.count();
        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(dag);
        SerializableDAG cloneDag2= (SerializableDAG) SerializationUtils.clone(dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        RandomSplitOperator randomSplitOperator = cloneDag.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        RandomSplitOperator.bitSet=new BitSet((int) count);
        randomSplitOperator.weights=weights;
        randomSplitOperator.count=count;
//        cloneDag.setInputPortAttribute(randomSplitOperator.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentOutputPort, randomSplitOperator.input);
        DefaultOutputPortSerializable currentSplitOutputPort2 = getCurrentOutputPort(cloneDag2);
        RandomSplitOperator randomSplitOperator2 = cloneDag2.addOperator(System.currentTimeMillis()+" RandomSplitter", RandomSplitOperator.class);
        randomSplitOperator2.weights=weights;
        randomSplitOperator2.flag=true;
        randomSplitOperator2.count=count;
//        cloneDag2.setInputPortAttribute(randomSplitOperator2.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        cloneDag2.addStream(System.currentTimeMillis()+" RandomSplit_Input Stream",currentSplitOutputPort2, randomSplitOperator2.input);
        ApexRDD<T> temp1= createClone(cloneDag);
        ApexRDD<T> temp2= createClone(cloneDag2);
        ApexRDD[] temp=new ApexRDD[]{temp1, temp2};
        return temp;
    }

    @Override
    public <U> RDD<Tuple2<T, U>> zip(RDD<U> other, ClassTag<U> evidence$9) {
        other.collect();
        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        ZipOperator<T,U> zipOperator = cloneDag.addOperator(System.currentTimeMillis()+ " ZipOperator " , new ZipOperator<T,U>());
        zipOperator.other= Arrays.asList((T[]) other.collect());
        zipOperator.count=zipOperator.other.size();
        cloneDag.addStream( System.currentTimeMillis()+ " ZipOperator ", currentOutputPort, zipOperator.input);
        enableParallelPartition(zipOperator,cloneDag);
        //cloneDag.setInputPortAttribute(z.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        ApexRDD<Tuple2<T,U>> temp= (ApexRDD<Tuple2<T,U>>) SerializationUtils.clone(this);
        temp.dag=cloneDag;
        return temp;
    }

    @Override
    public RDD<T> sample(boolean withReplacement, double fraction, long seed) {

        SerializableDAG cloneDag = (SerializableDAG) SerializationUtils.clone(this.dag);
        DefaultOutputPortSerializable currentOutputPort = getCurrentOutputPort(cloneDag);
        SampleOperator sampleOperator = cloneDag.addOperator(System.currentTimeMillis()+ " Map " , new SampleOperator());
        SampleOperator.fraction = fraction;
//        ScalaApexRDD$.MODULE$.test((ScalaApexRDD<Tuple2<Object, Object>>) this, (ClassTag<Object>) evidence$3,null,null);
        cloneDag.addStream( System.currentTimeMillis()+ " SampleOperatorStream ", currentOutputPort, sampleOperator.input);
        //cloneDag.setInputPortAttribute(m1.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
        enableParallelPartition(sampleOperator,cloneDag);
        return createClone(cloneDag);
    }

    @Override
    public <U> U withScope(Function0<U> body) {
        return (U) body;

    }
    public ApexRDD<T> createClone(SerializableDAG cloneDag){
        ApexRDD<T> apexRDDClone = (ApexRDD<T>) SerializationUtils.clone(this);
        apexRDDClone.dag =cloneDag;
        return apexRDDClone;
    }
    public void launch(SerializableDAG cloneDag, long runMillis, String name, boolean launchOnCluster) throws Exception {
        if(launchOnCluster)
            runDag(cloneDag,0,name);
        else
            runDagLocal(cloneDag,0,name);
    }
    public String getProperty(String prop){
        PathProperties properties = new PathProperties();
        return properties.getProperty(prop);

    }
    public void runDag(SerializableDAG cloneDag, long runMillis, String name) throws Exception {
        cloneDag.validate();
        String jars=getProperty("hjars");
        String appProperty =getProperty("ApexAppProperty");
        log.info("DAG successfully validated {}",name);
        Configuration conf = new Configuration(true);
        conf.set("fs.defaultFS","hdfs://localhost:54310");
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.addResource(new File(appProperty).toURI().toURL());
        conf.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME,jars);
        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag)    ;
        YarnConfiguration conf2 = new YarnConfiguration();
        YarnClient c = YarnClient.createYarnClient();
        c.init(conf);
        c.start();

//        List<ApplicationReport> appsReport = c.getApplications(states);
//        appsReport.get(0).getApplicationId();
        StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
        appLauncher.loadDependencies();
        StreamingAppFactory appFactory = new StreamingAppFactory(app, name);

        ApplicationId id = appLauncher.launchApp(appFactory);
        while(!successFileExists()) {
            log.info("Waiting for the _SUCCESS file");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        c.killApplication(id);

        if(appId!=null)
            //deleteJars(id.toString());
        appId=id.toString();
        log.info(" Address {}",conf.get("yarn.resourcemanager.address"));
    }
    public void deleteJars(String path){
        Configuration conf = new Configuration();
        Path pt=new Path("hdfs://localhost:54310/user/anurag/datatorrent/apps/"+path);
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(pt.toUri(), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            hdfs.delete(pt, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("Deleted jars from {}",path);
    }
    public String appId;
   /* public synchronized static void deleteSUCCESSFile() {
        try {
            alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
            AlluxioURI pathURI=new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
            if(fs.exists(pathURI)) fs.delete(pathURI);

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }

    }
    public  boolean successFileExists(){

        alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
        AlluxioURI pathURI=new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
        try {
            return fs.exists(pathURI);
        } catch (IOException | AlluxioException e) {
            throw new RuntimeException(e);
        }

    }*/
    public void     runDagLocal(SerializableDAG cloneDag, long runMillis, String name) throws IOException, AlluxioException, InterruptedException {
        cloneDag.validate();
        log.info("DAG successfully validated {}",name);
        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(false);
        GenericApplication app = new GenericApplication();
        app.setDag(cloneDag);
        try {
            lma.prepareDAG(app, conf);
        } catch (Exception e) {
            throw new RuntimeException("Exception in prepareDAG", e);
        }
        LocalMode.Controller lc = lma.getController();
//        File successFile = new File("/tmp/spark-apex/_SUCCESS");
//        if(successFile.exists())    successFile.delete();
        lc.runAsync();
        while(!successFileExists()) {
            log.info("Waiting for the _SUCCESS file");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lc.shutdown();


    }
    public enum OperatorType {
        INPUT,
        PROCESS,
        OUTPUT
    }
}
