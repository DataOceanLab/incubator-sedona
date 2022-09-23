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


object RangeQueryTest extends App{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  var total_time_raw=0.0
  var total_time_new=0.0

	var sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
		config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName).
		master("local[*]").appName("SedonaSQL-demo").getOrCreate()

	//SedonaSQLRegistrator.registerAll(sparkSession)
  //SedonaVizRegistrator.registerAll(sparkSession)

  var count = 0;
  var exp=10;
  val bwRangeQ = new BufferedWriter(new PrintWriter(new File("/Users/abrarabir/Desktop/testRangeQD.csv")))
  bwRangeQ.append("file_name,avg_time,lb_old,lb_new,method\n" )

  //testSpatialRangeQueryRaw()
  //testSpatialRangeQueryNewM()
  //testOldSpatialRangeQueryMSingle()
  testSpatialRangeQueryNewMSingle()
  testOldSpatialRangeQueryMSingle()
  //Raw Avg Time: 0.0015963142366666665 NewM avg time: 0.0013980155966666667 3.5 M Data
  // Raw Avg Time: 0.0018857716449999999 NewM avg time: 0.0014157011383333334 5M Data (took 43 min for spatial partitioning)

  bwRangeQ.close()

  println("Raw Avg Time: "+ total_time_raw/exp.toDouble + " NewM avg time: "+ total_time_new/exp.toDouble)
  System.out.println("All SedonaSQL DEMOs passed!")


  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  def testSpatialRangeQueryNewMSingle(): Unit ={
    val polygonFile="/Users/abrarabir/Desktop/new-spjoin-data/diagonal_1MC_1_PL.wkt"

    val carryOtherAttributes = false // Carry Column 2 (hotel, gas, bar...)
    val polygonRDDSplitter = FileDataSplitter.WKT

    total_time_new=0.0;
    val objectRDD = new RectangleRDD(sparkSession.sparkContext, polygonFile, polygonRDDSplitter, carryOtherAttributes)
    objectRDD.analyze()

    val bx=objectRDD.boundaryEnvelope.getMaxX()-objectRDD.boundaryEnvelope.getMinX()
    val by=objectRDD.boundaryEnvelope.getMaxY()-objectRDD.boundaryEnvelope.getMinY()
    val avgOX=objectRDD.avgLenX
    val avgOY=objectRDD.avgLenY
    print(" Height: "+(avgOX/bx)+"  Width:"+(avgOY/by))

    val rangeQueryWindow = new Envelope(0.5875372573942312, 0.9479623713261134, 0.5879125121987439, 0.9407688494932558)

    val t0 = System.nanoTime()
    objectRDD.spatialPartitioning(GridType.KDBTREE)

    objectRDD.spatialPartitionedRDDN2.persist(StorageLevel.MEMORY_ONLY)
    var useIndex=false
    //* Using Index
    objectRDD.buildIndexN(IndexType.QUADTREE, true) // This must be true if we build index on spatial partitioning
    objectRDD.indexedRDDN.persist(StorageLevel.MEMORY_ONLY)
    useIndex=true
    // */
    //0.0016269061900000003


    //var t0 = System.nanoTime()

    val resultSizeN = RangeQuery.SpatialRangeQueryN(objectRDD, rangeQueryWindow, false, useIndex).count
    val t1 = System.nanoTime()
    val elapsedTimeNew = ((t1 - t0) / 1000000000.00) / 60.00
    System.out.println("New Method Res: "+resultSizeN+" elapsed time: "+elapsedTimeNew)
    //total_time_new=total_time_new+elapsedTimeNew
    for( a <- 1 to exp){
      val t0 = System.nanoTime()
      val resultSizeN = RangeQuery.SpatialRangeQueryN(objectRDD, rangeQueryWindow, false, useIndex).count
      val t1 = System.nanoTime()
      val elapsedTimeNew = ((t1 - t0) / 1000000000.00) / 60.00
      System.out.println("New Method Res: "+resultSizeN+" elapsed time: "+elapsedTimeNew)
      total_time_new=total_time_new+elapsedTimeNew
    }
  }
  def testOldSpatialRangeQueryMSingle(): Unit ={
    val polygonFile="/Users/abrarabir/Desktop/new-spjoin-data/diagonal_1MC_1_PL.wkt"

    val polygonRDDSplitter = FileDataSplitter.WKT
    val carryOtherAttributes = false // Carry Column 2 (hotel, gas, bar...)
    total_time_raw=0.0
    val objectRDD = new RectangleRDD(sparkSession.sparkContext, polygonFile, polygonRDDSplitter, carryOtherAttributes)

    val rangeQueryWindow = new Envelope(0.5875372573942312, 0.9479623713261134, 0.5879125121987439, 0.9407688494932558)
    objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
    var useIndex=false
    /* using index
    objectRDD.buildIndex(IndexType.QUADTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)
    useIndex=true
    // */
    val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, useIndex).count
    for( a <- 1 to exp){
      var t0 = System.nanoTime()
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, useIndex).count
      var t1 = System.nanoTime()
      val elapsedTime = ((t1 - t0) / 1000000000.00) / 60.00
      total_time_raw=total_time_raw+elapsedTime
      System.out.println("Original Res (RAW): "+resultSize+" elapsed time: "+elapsedTime)//0.004944192000000001 0.08450179009666665
    }
  }
  def testSpatialRangeQueryNewM() {
    //val polygonFile="/Users/abrarabir/Desktop/new-spjoin-data/diagonal_1MC_1_PL.wkt"
    val plFiles=getListOfFiles("/Users/abrarabir/Desktop/new-spjoin-data/testRangeQuery")
    for ( polygon <- plFiles ) {
      val polygonFile=polygon.toString()
      total_time_new=0.0

      val polygonRDDSplitter = FileDataSplitter.WKT
      val carryOtherAttributes = false // Carry Column 2 (hotel, gas, bar...)

      val objectRDD = new RectangleRDD(sparkSession.sparkContext, polygonFile, polygonRDDSplitter, carryOtherAttributes)
      objectRDD.analyze()

      val rangeQueryWindow = new Envelope(0.5875372573942312, 0.9479623713261134, 0.5879125121987439, 0.9407688494932558)

      val t0 = System.nanoTime()
      objectRDD.spatialPartitioning(GridType.KDBTREE)
      objectRDD.spatialPartitionedRDDN2.persist(StorageLevel.MEMORY_ONLY)

      //var t0 = System.nanoTime()

      val resultSizeN = RangeQuery.SpatialRangeQueryN(objectRDD, rangeQueryWindow, false, false).count
      val t1 = System.nanoTime()
      val elapsedTimeNew = ((t1 - t0) / 1000000000.00) / 60.00
      System.out.println("New Method Res: "+resultSizeN+" elapsed time: "+elapsedTimeNew)
      total_time_new=total_time_new+elapsedTimeNew
      for( a <- 1 to 4){
        val t0 = System.nanoTime()
        val resultSizeN = RangeQuery.SpatialRangeQueryN(objectRDD, rangeQueryWindow, false, false).count
        val t1 = System.nanoTime()
        val elapsedTimeNew = ((t1 - t0) / 1000000000.00) / 60.00
        System.out.println("New Method Res: "+resultSizeN+" elapsed time: "+elapsedTimeNew)
        total_time_new=total_time_new+elapsedTimeNew
      }
      bwRangeQ.append(polygon.getName()+","+total_time_new/5.0+","+objectRDD.load_balanceO+","+objectRDD.load_balanceN+",N\n")
    }


  }

  def testSpatialRangeQueryRaw() {
    //val polygonFile="/Users/abrarabir/Desktop/new-spjoin-data/diagonal_1MC_1_PL.wkt"
    val polygonFiles=getListOfFiles("/Users/abrarabir/Desktop/new-spjoin-data/testRangeQuery")
    for ( polygon <- polygonFiles ) {
      val polygonFile=polygon.toString()
      total_time_raw=0.0
      val polygonRDDSplitter = FileDataSplitter.WKT
      val carryOtherAttributes = false // Carry Column 2 (hotel, gas, bar...)

      val objectRDD = new RectangleRDD(sparkSession.sparkContext, polygonFile, polygonRDDSplitter, carryOtherAttributes)
      objectRDD.analyze()
      val rangeQueryWindow = new Envelope(0.5875372573942312, 0.9479623713261134, 0.5879125121987439, 0.9407688494932558)
      objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)

      for( a <- 1 to 5){
        var t0 = System.nanoTime()
        val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count
        var t1 = System.nanoTime()
        val elapsedTime = ((t1 - t0) / 1000000000.00) / 60.00
        total_time_raw=total_time_raw+elapsedTime
        System.out.println("Original Res (RAW): "+resultSize+" elapsed time: "+elapsedTime)
      }
      bwRangeQ.append(polygon.getName()+","+total_time_raw/5.0+","+objectRDD.load_balanceO+","+objectRDD.load_balanceN+",O\n")


    }


  }



}