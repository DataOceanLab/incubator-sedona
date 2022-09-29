/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.core.showcase

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialOperator.{JoinQuery, RangeQuery}
import org.apache.sedona.core.spatialRDD.{PointRDD, RectangleRDD, SpatialRDD}
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.{Envelope, Polygon}

import java.io.{BufferedWriter, File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.control.Breaks.{break, breakable}


object SpatialJoinTest extends App{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  var total_time_raw=0.0
  var total_time_new=0.0

	var sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
		config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    .config("spark.executor.memory","110g")
    .config("spark.driver.memory","110g")
    .master("local[*]")
    .appName("SedonaSQL-demo")
    .getOrCreate()

	//SedonaSQLRegistrator.registerAll(sparkSession)
  //SedonaVizRegistrator.registerAll(sparkSession)


  val csvPolygonD1 = args(0).toString
  val csvPolygonD2 = args(1).toString

  //val bufferedPrintWriter = new BufferedWriter(new PrintWriter(new File("/Users/abrarabir/Desktop/JoinIncPartLarge_01.csv")))

  var polygonFilesD1=getListOfFiles(csvPolygonD1)
  var polygonFilesD2=getListOfFiles(csvPolygonD2)
  var count = 0;
  //bufferedPrintWriter.append("file_name,rwcount,spartial_count,br_count,br_rect_ratio,duplicates,duplicate_ratio,avgMBRArea,avgLenX,avgLenY,E_0,E_2,total_cells,TT_area,TT_margin,TT_overlap,load_balance,load_balance_new,load_balance_brdd \n")
//  bufferedPrintWriter.append("file_name,rwcount,spartial_count,br_count,br_rect_ratio,requiredFromBRDD," +
//    "duplicates,duplicate_ratio,num_partition,load_balanceO,load_balance_new,load_balance_brdd," +
//    "file_name,rwcount,spartial_count,br_count,br_rect_ratio,requiredFromBRDD,duplicates,duplicate_ratio,num_partition,load_balanceO,load_balance_new,load_balance_brdd,elaspsedTimeOld, elaspsedTimeNew, joinCard \n")
  System.out.println("DataDic: file_name,rwcount,spatial_count,br_count,br_rect_ratio,requiredFromBRDD," +
    "duplicates,duplicate_ratio,avgMBRArea,avgLenX,avgLenY,E_0,E_2,TT_area,TT_margin,TT_overlap,num_partition,load_balanceO,load_balance_new,load_balance_brdd," +
    "file_name,rwcount,spartial_count,br_count,br_rect_ratio,requiredFromBRDD,duplicates,duplicate_ratio," +
    "avgMBRArea,avgLenX,avgLenY,E_0,E_2,TT_area,TT_margin,TT_overlap,num_partition,load_balanceO,load_balance_new,load_balance_brdd,overlapMBRFirstDS," +
    "overlapMBRSecondDS,jaccardSimilarity,elaspsedTimeOld, elaspsedTimeNew, joinCard \n")
  breakable{
    for ( plD1 <- polygonFilesD1 ) {
      for ( plD2 <- polygonFilesD2 ) {
        if(plD1.getName().endsWith(".wkt") & plD2.getName().endsWith(".wkt"))
          {
            count=count+1;
            println(plD1.getName()+"  "+plD2.getName()+" "+ count)
            //try {
                testNewRangeJoinQueryNFileparam(plD1, plD2)
            //}
            //catch {
            //  case ex: Throwable => println("Found an exception: " + ex)
              //println("spoData: "+appName+","+startTime +","+endTime+","+ptCardinal+","+plCardinal+","+pointRDD.spatialPartitionedRDD.rdd.getNumPartitions+","+polygonRDD.spatialPartitionedRDD.rdd.getNumPartitions+","++"");

            //}
          }
        if(count==1){
          //break
        }

      }
      if(count==1){
        //break
      }
    }
  }
  //bufferedPrintWriter.close()
  System.out.println("Count: "+count);

  System.out.println("All SedonaSQL DEMOs passed!")


  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  def testNewRangeJoinQueryNFileparam(polgonFileD1: File,polgonFileD2: File):Unit = {
    val wktPolygonFileNameD1 = polgonFileD1.toString()
    val wktPolygonFileNameD2 = polgonFileD2.toString()

    val carryOtherAttributes = false // Carry Column 2 (hotel, gas, bar...)

    val polygonRDDInputLocation = wktPolygonFileNameD1
    val polygonRDDSplitter = FileDataSplitter.WKT
    var polygonRDD = new RectangleRDD(sparkSession.sparkContext, polygonRDDInputLocation, polygonRDDSplitter, carryOtherAttributes)
    polygonRDD.rawSpatialRDD=polygonRDD.rawSpatialRDD.repartition(10)

    val polygonRDDInputLocation2 = wktPolygonFileNameD2
    val polygonRDDSplitter2 = FileDataSplitter.WKT
    var polygonRDD2 = new RectangleRDD(sparkSession.sparkContext, polygonRDDInputLocation2, polygonRDDSplitter2, carryOtherAttributes)
    polygonRDD2.rawSpatialRDD=polygonRDD2.rawSpatialRDD.repartition(10);


    val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex = false

    polygonRDD.analyze()
    polygonRDD.analyze(Array[Double](0.1,0.2,0.3,0.5,0.6))
    polygonRDD2.analyze()
    polygonRDD2.analyze(Array[Double](0.1,0.2,0.3,0.5,0.6))


    //pointRDD.analyzeCombinedStats(polygonRDD)
    polygonRDD.boundaryEnvelope.expandToInclude(polygonRDD2.boundaryEnvelope)
    polygonRDD2.boundaryEnvelope.expandToInclude(polygonRDD.boundaryEnvelope)
    polygonRDD.spatialPartitioning(GridType.KDBTREE)
    polygonRDD2.spatialPartitioning(polygonRDD.getPartitioner)

    val polySpatialRDD = new SpatialRDD[Polygon]
    //val spatialRDD = new SpatialRDD[_ <: Geometry]
    polySpatialRDD.rawSpatialRDD=polygonRDD.rawSpatialRDD;
    polySpatialRDD.analyze()
    polygonRDD2.analyzeCombinedStats(polySpatialRDD)
    println("")
    //polygonRDD.spatialPartitioning(GridType.KDBTREE)
    //polygonRDD2.spatialPartitioning(polygonRDD.getPartitioner)

    val rwCntD1=polygonRDD.rawSpatialRDD.count();
    val spCntD1=polygonRDD.spatialPartitionedRDDN2.count();
    val brCntD1=polygonRDD.boundaryRectRDD.count();

    val rwCntD2=polygonRDD2.rawSpatialRDD.count();
    val spCntD2=polygonRDD2.spatialPartitionedRDDN2.count();
    val brCntD2=polygonRDD2.boundaryRectRDD.count();

    var joinCardN=0L;
    var joinCardO=0L;
    var elapsedTimeOld=0.0;
    var elapsedTimeNew=0.0;
    total_time_raw=0.0
    total_time_new=0.0
    val num_exp=5
    for( a <- 1 to num_exp){
      var t0 = System.nanoTime()
      val result= JoinQuery.SpatialJoinQueryFlat(polygonRDD2,polygonRDD,usingIndex,considerBoundaryIntersection);
      joinCardO=result.count();
      var t1 = System.nanoTime()
      elapsedTimeOld = ((t1 - t0) / 1000000000.00) / 60.00
      total_time_raw=total_time_raw+elapsedTimeOld
      t0 = System.nanoTime()
      val resultN= JoinQuery.SpatialJoinQueryFlatN(polygonRDD2,polygonRDD,usingIndex,considerBoundaryIntersection);
      joinCardN=resultN.count();
      t1 = System.nanoTime()
      elapsedTimeNew = ((t1 - t0) / 1000000000.00) / 60.00
      total_time_new=total_time_new+elapsedTimeNew
    }
    println("Original Res: "+joinCardO+" elapsed time: "+(total_time_raw/num_exp.toDouble)+" New Res: "+joinCardN+" Elapsed time new M: "+(total_time_new/num_exp.toDouble))
    val data=
      polgonFileD1.getName()+","+
        rwCntD1+","+
        spCntD1+","+
        brCntD1+","+
        (brCntD1.toDouble/rwCntD1.toDouble)+","+ //boundary rectangle ratio
        polygonRDD.requiredFromBRDD+","+
        (spCntD1-rwCntD1)+","+ //number of duplicates
        ((spCntD1-rwCntD1).toDouble/rwCntD1.toDouble)+","+ //duplicate ratio
        polygonRDD.avgMBRArea+","+
        polygonRDD.avgLenX+","+
        polygonRDD.avgLenY+","+
        polygonRDD.E_0+","+
        polygonRDD.E_2+","+
        polygonRDD.TT_area+","+
        polygonRDD.TT_margin+","+
        polygonRDD.TT_overlap+","+
        polygonRDD.spatialPartitionedRDDN2.getNumPartitions+","+
        polygonRDD.load_balanceO+","+
        polygonRDD.load_balanceN+","+
        polygonRDD.load_balanceBRN+","+
        polgonFileD2.getName()+","+
        rwCntD2+","+
        spCntD2+","+
        brCntD2+","+
        (brCntD2.toDouble/rwCntD2.toDouble)+","+ //boundary rectangle ratio
        polygonRDD2.requiredFromBRDD+","+
        (spCntD2-rwCntD2)+","+ //number of duplicates
        ((spCntD2-rwCntD2).toDouble/rwCntD2.toDouble)+","+ //duplicate ratio
        polygonRDD2.avgMBRArea+","+
        polygonRDD2.avgLenX+","+
        polygonRDD2.avgLenY+","+
        polygonRDD2.E_0+","+
        polygonRDD2.E_2+","+
        polygonRDD2.TT_area+","+
        polygonRDD2.TT_margin+","+
        polygonRDD2.TT_overlap+","+
        polygonRDD2.spatialPartitionedRDDN2.getNumPartitions+","+
        polygonRDD2.load_balanceO+","+
        polygonRDD2.load_balanceN+","+
        polygonRDD2.load_balanceBRN+","+
        polygonRDD2.overlapMBRFirstDS+","+
        polygonRDD2.overlapMBRSecondDS+","+
        polygonRDD2.jaccardSimilarity+","+
        elapsedTimeOld+","+
        elapsedTimeNew+","+
        joinCardN;
    System.out.println("spodata: "+data);

    //bufferedPrintWriter.append(data+"\n")

  }

  def newPartitioningInfo(polgonFile: File):Unit = {
    val wktPolygonFileName = polgonFile.toString();


    val carryOtherAttributes = false // Carry Column 2 (hotel, gas, bar...)
    val polygonRDDInputLocation = wktPolygonFileName
    val polygonRDDSplitter = FileDataSplitter.WKT
    var polygonRDD = new RectangleRDD(sparkSession.sparkContext, polygonRDDInputLocation, polygonRDDSplitter, carryOtherAttributes)
    polygonRDD.analyze()
    polygonRDD.analyze(Array[Double](0.1,0.2,0.3,0.5,0.6))

    polygonRDD.spatialPartitioning(GridType.KDBTREE);
    polygonRDD.analyzePartitionN();

    val rwCnt=polygonRDD.rawSpatialRDD.count();
    val spCnt=polygonRDD.spatialPartitionedRDDN2.count();
    val brCnt=polygonRDD.boundaryRectRDD.count();
    val data=
      polgonFile.getName()+","+
      rwCnt+","+
      spCnt+","+
      brCnt+","+
      (brCnt.toDouble/rwCnt.toDouble)+","+ //boundary rectangle ratio
      (spCnt-rwCnt)+","+ //number of duplicates
      ((spCnt-rwCnt).toDouble/rwCnt.toDouble)+","+ //duplicate ratio
      polygonRDD.avgMBRArea+","+
      polygonRDD.avgLenX+","+
      polygonRDD.avgLenY+","+
      polygonRDD.E_0+","+
      polygonRDD.E_2+","+
      polygonRDD.total_cells+","+
      polygonRDD.TT_area+","+
      polygonRDD.TT_margin+","+
      polygonRDD.TT_overlap+","+
      polygonRDD.load_balanceO+","+
      polygonRDD.load_balanceN+","+
      polygonRDD.load_balanceBRN;
    System.out.println(data);
    //bufferedPrintWriter.append(data+"\n")


  }



}