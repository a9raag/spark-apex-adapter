package org.apache.apex.adapters.spark.apexscala

import java.io.IOException

import alluxio.AlluxioURI
import alluxio.exception.AlluxioException
import com.datatorrent.api.LocalMode
import org.apache.apex.adapters.spark.ApexRDD
import org.apache.apex.adapters.spark.io.ReadFromFS._
import org.apache.apex.adapters.spark.operators._
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by anurag on 16/12/16.
  */

 class ScalaApexRDD[T:ClassTag](
                                 @transient private var sc: SparkContext,
                                 @transient private var deps: Seq[Dependency[_]]
                              ) extends RDD[T](sc,Nil) with Serializable{

  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))
  val dag=new SerializableDAG()

  def getFunc[U](f: (Iterator[T]) => Iterator[U]): (TaskContext, Int, Iterator[T]) => Iterator[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(iter)
    func
  }


//  def getFunc(f: (T, T) => T): (Iterator[T]) => Option[T] = {
//    val reducePartition: Iterator[T] => Option[T] = iter => {
//      if (iter.hasNext) {
//        Some(iter.reduceLeft(f))
//      } else {
//        None
//      }
//    }
//    reducePartition
//  }



//  def toArray(array: Array[Object]): Array[T] ={
//  val data= new Array[T](array.length)
//  val count=0
//  for ( a <- array.toIterator){
//    data(count)=a.asInstanceOf[T]
//  }
//  data
//
//}


  override def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")

    val cleanSeqOp = seqOp
    val cleanCombOp = combOp
    val aggregatePartition =
      (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    var partiallyAggregated = mapPartitions(it => Iterator(aggregatePartition(it)))
    var numPartitions = partiallyAggregated.partitions.length
    val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
    // If creating an extra level doesn't help reduce
    // the wall-clock time, we stop tree aggregation.

//    // Don't trigger TreeAggregation when it doesn't save wall-clock time
//    while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
//      numPartitions /= scale
//      val curNumPartitions = numPartitions
//      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
//        (i, iter) => iter.map((i % curNumPartitions, _))
//      }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values
//    }
     partiallyAggregated.first()
}

//  override def take(num: Int): Array[T] = {
//    println(num)
//    var a = new  util.ArrayList[T]()
////    var a =new ArrayBuffer[T](num)
//    if(num==1){
//
//      a.add(this.collect()(0))
//      println("Collected element: " + a)
//      a.toArray
////      return a
//    }
//    var count=0;
////    for( o <-this.collect()){
////      if(count>=num)
////        return a.toArray
////      else{
////        a+=o
////        count=count+1
////      }
////    }
////   a.toArray
//    return scala.collection.JavaConversions.asScalaBuffer(a).toArray
//  }
def getCurrentOutputPort(cloneDag: SerializableDAG): DefaultOutputPortSerializable[_] = {
  try {
    log.debug("Last operator in the Dag {}", this.asInstanceOf[ApexRDD[T]].getDag().getLastOperatorName)
    val currentOperator = cloneDag.getOperatorMeta(cloneDag.getLastOperatorName).getOperator.asInstanceOf[BaseOperatorSerializable[_]]
    currentOperator.getOutputPort
  }
  catch {
    case e: Exception => {
      throw new RuntimeException(e)
    }
  }
}
//
//  override def take(num: Int): Array[T] = {
//
//  val cloneDag = SerializationUtils.clone(this.asInstanceOf[ApexRDD[T]].getDag()).asInstanceOf[SerializableDAG]
//
//  val currentOutputPort = getCurrentOutputPort(cloneDag)
//  val takeOperator = cloneDag.addOperator(System.currentTimeMillis + " Take Operator", classOf[TakeOperator[T]])
//  takeOperator.count = num
//    var ambiguous = cloneDag.getClass.getMethods.filter(_.getName == "addStream")
//    var wanted = ambiguous.find(_.getParameterTypes.length == 3).get
//    wanted.invoke(cloneDag, System.currentTimeMillis()+" Take Stream",currentOutputPort,takeOperator.input)
////  cloneDag.addStream(System.currentTimeMillis + " Take Stream", currentOutputPort, takeOperator.input)
//  val writer = cloneDag.addOperator(System.currentTimeMillis + " FileWriter", classOf[FileWriterOperator])
//  //cloneDag.setInputPortAttribute(writer.input, Context.PortContext.STREAM_CODEC, new JavaSerializationStreamCodec());
//  writer.setAbsoluteFilePath("selectedData")
//  ambiguous = cloneDag.getClass.getMethods.filter(_.getName == "addStream")
//  wanted = ambiguous.find(_.getParameterTypes.length == 3).get
//  wanted.invoke(cloneDag, System.currentTimeMillis()+" File Stream",takeOperator.output,writer.input)
////  cloneDag.addStream(System.currentTimeMillis + "FileWriterStream", takeOperator.output, writer.input)
//  try
//    runDagLocal(cloneDag, 3000, "take")
//
//  catch {
//    case e: Exception => {
//      e.printStackTrace()
//    }
//  }
//  val array = ApexRDD.readFromAlluxio("selectedData").asInstanceOf[util.ArrayList[T]].toArray()
//  ApexRDD.deleteSUCCESSFile()
//    val temp = (array.asInstanceOf[Array[T]]).headOption
//    println(temp)
//    val value = temp.getOrElse {
//      throw new IllegalArgumentException(s"DecisionTree requires size of input RDD > 0, " +
//        s"but was given by empty one.")
//    }
//    println(value)
//    ApexRDD.deleteSUCCESSFile()
//    return (array.asInstanceOf[Array[T]])
////    val buf = new Array[T](num)
//    val buf = new ArrayBuffer[T]
//    for (c<- array){
//      buf.append(c.asInstanceOf[T])
//    }
//    return array.map(_.asInstanceOf[T]).array
////    var b = None: Option[Array[T]]
////    try {
//  //        b = Some(array.map(_.asInstanceOf[T]).array)
//  //        b.get
//  //    }
////    catch {
////
////      case e: Exception => {
////        println("Exception Occurred "+ b)
////        throw new RuntimeException(e)
////      }
////    }
//
//
//
//}
  override def keyBy[K](f: (T) => K): RDD[(K, T)] ={
    this.map(x => (f(x), x))
  }

  override def first: T = this.take(1)(0)
  /*def successFileExists: Boolean = {
    val fs = alluxio.client.file.FileSystem.Factory.get
    val pathURI = new AlluxioURI("/user/anurag/spark-apex/_SUCCESS")
    try
      fs.exists(pathURI)

    catch {
      case e: Any => {
        throw new RuntimeException(e)
      }
    }
  }*/
  @throws[IOException]
  @throws[AlluxioException]
  @throws[InterruptedException]
  def runDagLocal(cloneDag: SerializableDAG, runMillis: Long, name: String) {
    cloneDag.validate()
    log.info("DAG successfully validated {}", name)
    val lma = LocalMode.newInstance
    val conf = new Configuration(false)
    val app = new GenericApplication
    app.setDag(cloneDag)
    try
      lma.prepareDAG(app, conf)

    catch {
      case e: Exception => {
        throw new RuntimeException("Exception in prepareDAG", e)
      }
    }
    val lc = lma.getController
    //        File successFile = new File("/tmp/spark-apex/_SUCCESS");
    //        if(successFile.exists())    successFile.delete();
    lc.runAsync()
    while (!successFileExists) {
      log.info("Waiting for the _SUCCESS file")
      try
        Thread.sleep(10)

      catch {
        case e: InterruptedException => {
          e.printStackTrace()
        }
      }
    }
    lc.shutdown()
  }
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = ???

  override protected def getPartitions: Array[Partition] = ???
}
object ScalaApexRDD {

}
