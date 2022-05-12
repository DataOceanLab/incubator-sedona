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

package org.apache.sedona.core.spatialRDD;

import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialPartitioning.*;
import org.apache.sedona.core.spatialPartitioning.quadtree.StandardQuadTree;
import org.apache.sedona.core.spatialRddTool.IndexBuilder;
import org.apache.sedona.core.spatialRddTool.StatCalculator;
import org.apache.sedona.core.utils.GeomUtils;
import org.apache.sedona.core.utils.RDDSampleUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.random.SamplingUtils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.wololo.geojson.Feature;
import org.wololo.jts2geojson.GeoJSONWriter;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;
import visad.Tuple;

import java.io.Serializable;
import java.util.*;

// TODO: Auto-generated Javadoc

/**
 * The Class SpatialRDD.
 */
public class SpatialRDD<T extends Geometry>
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(SpatialRDD.class);

    /**
     * The total number of records.
     */
    public long approximateTotalCount = -1;
    /**
     * The average mbrArea of the records.
     */
    public double avgMBRArea = -1;
    /**
     * The average lenX of the records.
     */
    public double avgLenX = -1;
    /**
     * The average lenY  of the records.
     */
    public double avgLenY = -1;
    /**
     * E0.
     */
    public double E_0 = -1;
    /**
     * E2.
     */
    public double E_2 = -1;
    /**
     * The boundary envelope.
     */
    public Envelope boundaryEnvelope = null;

    /**
     * The spatial partitioned RDD.
     */
    public JavaRDD<T> spatialPartitionedRDD;

    /**
     * The indexed RDD.
     */
    public JavaRDD<SpatialIndex> indexedRDD;

    /**
     * The indexed raw RDD.
     */
    public JavaRDD<SpatialIndex> indexedRawRDD;

    /**
     * The raw spatial RDD.
     */
    public JavaRDD<T> rawSpatialRDD;

    public List<String> fieldNames;
    /**
     * The CR stransformation.
     */
    protected boolean CRStransformation = false;
    /**
     * The source epsg code.
     */
    protected String sourceEpsgCode = "";
    /**
     * The target epgsg code.
     */
    protected String targetEpgsgCode = "";
    private SpatialPartitioner partitioner;
    /**
     * The sample number.
     */
    private int sampleNumber = -1;

    public int getSampleNumber()
    {
        return sampleNumber;
    }

    /**
     * Sets the sample number.
     *
     * @param sampleNumber the new sample number
     */
    public void setSampleNumber(int sampleNumber)
    {
        this.sampleNumber = sampleNumber;
    }

    /**
     * CRS transform.
     *
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCRSCode the target epsg CRS code
     * @param lenient consider the difference of the geodetic datum between the two coordinate systems,
     * if {@code true}, never throw an exception "Bursa-Wolf Parameters Required", but not
     * recommended for careful analysis work
     * @return true, if successful
     */
    public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode, boolean lenient)
    {
        try {
            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
            final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
            this.CRStransformation = true;
            this.sourceEpsgCode = sourceEpsgCRSCode;
            this.targetEpgsgCode = targetEpsgCRSCode;
            this.rawSpatialRDD = this.rawSpatialRDD.map(new Function<T, T>()
            {
                @Override
                public T call(T originalObject)
                        throws Exception
                {
                    return (T) JTS.transform(originalObject, transform);
                }
            });
            return true;
        }
        catch (FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
    }

    /**
     * CRS transform.
     *
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCRSCode the target epsg CRS code
     * @return true, if successful
     */
    public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode)
    {
        return CRSTransform(sourceEpsgCRSCode, targetEpsgCRSCode, false);
    }

    public boolean spatialPartitioning(GridType gridType)
            throws Exception
    {
        int numPartitions = this.rawSpatialRDD.rdd().partitions().length;
        spatialPartitioning(gridType, numPartitions);
        return true;
    }

    /**
     * Spatial partitioning.
     *
     * @param gridType the grid type
     * @return true, if successful
     * @throws Exception the exception
     */
    public void calc_partitioner(GridType gridType, int numPartitions)
            throws Exception
    {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        if (this.boundaryEnvelope == null) {
            throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call analyze() first.");
        }
        if (this.approximateTotalCount == -1) {
            throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD total count is unkown. Please call analyze() first.");
        }

        //Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.approximateTotalCount, this.sampleNumber);
        //Take Sample
        // RDD.takeSample implementation tends to scan the data multiple times to gather the exact
        // number of samples requested. Repeated scans increase the latency of the join. This increase
        // is significant for large datasets.
        // See https://github.com/apache/spark/blob/412b0e8969215411b97efd3d0984dc6cac5d31e0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L508
        // Here, we choose to get samples faster over getting exactly specified number of samples.
        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, approximateTotalCount, false);
        List<Envelope> samples = this.rawSpatialRDD.sample(false, fraction)
                .map(new Function<T, Envelope>()
                {
                    @Override
                    public Envelope call(T geometry)
                            throws Exception
                    {
                        return geometry.getEnvelopeInternal();
                    }
                })
                .collect();

        logger.info("Collected " + samples.size() + " samples");

        // Add some padding at the top and right of the boundaryEnvelope to make
        // sure all geometries lie within the half-open rectangle.
        final Envelope paddedBoundary = new Envelope(
                boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

        switch (gridType) {
            case EQUALGRID: {
                // Force the quad-tree to grow up to a certain level
                // So the actual num of partitions might be slightly different
                int minLevel = (int) Math.max(Math.log(numPartitions)/Math.log(4), 0);
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(new ArrayList<Envelope>(), paddedBoundary,
                        numPartitions, minLevel);
                StandardQuadTree tree = quadtreePartitioning.getPartitionTree();
                partitioner = new QuadTreePartitioner(tree);
                break;
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(samples, paddedBoundary, numPartitions);
                StandardQuadTree tree = quadtreePartitioning.getPartitionTree();
                partitioner = new QuadTreePartitioner(tree);
                break;
            }
            case KDBTREE: {
                final KDB tree = new KDB(samples.size() / numPartitions, numPartitions, paddedBoundary);
                for (final Envelope sample : samples) {
                    tree.insert(sample);
                }
                tree.assignLeafIds();
                partitioner = new KDBTreePartitioner(tree);
                break;
            }
            default:
                throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method. " +
                        "The following partitioning methods are not longer supported: R-Tree, Hilbert curve, Voronoi");
        }
    }

    public void spatialPartitioning(GridType gridType, int numPartitions)
            throws Exception
    {
        calc_partitioner(gridType, numPartitions);
        this.spatialPartitionedRDD = partition(partitioner);
    }

    public SpatialPartitioner getPartitioner()
    {
        return partitioner;
    }

    public void spatialPartitioning(SpatialPartitioner partitioner)
    {
        this.partitioner = partitioner;
        this.spatialPartitionedRDD = partition(partitioner);
    }

    /**
     * @deprecated Use spatialPartitioning(SpatialPartitioner partitioner)
     */
    public boolean spatialPartitioning(final List<Envelope> otherGrids)
            throws Exception
    {
        this.partitioner = new FlatGridPartitioner(otherGrids);
        this.spatialPartitionedRDD = partition(partitioner);
        return true;
    }

    /**
     * @deprecated Use spatialPartitioning(SpatialPartitioner partitioner)
     */
    public boolean spatialPartitioning(final StandardQuadTree partitionTree)
            throws Exception
    {
        this.partitioner = new QuadTreePartitioner(partitionTree);
        this.spatialPartitionedRDD = partition(partitioner);
        return true;
    }

    private JavaRDD<T> partition(final SpatialPartitioner partitioner)
    {
        return this.rawSpatialRDD.flatMapToPair(
                new PairFlatMapFunction<T, Integer, T>()
                {
                    @Override
                    public Iterator<Tuple2<Integer, T>> call(T spatialObject)
                            throws Exception
                    {
                        return partitioner.placeObject(spatialObject);
                    }
                }
        ).partitionBy(partitioner)
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, T>>, T>()
                {
                    @Override
                    public Iterator<T> call(final Iterator<Tuple2<Integer, T>> tuple2Iterator)
                            throws Exception
                    {
                        return new Iterator<T>()
                        {
                            @Override
                            public boolean hasNext()
                            {
                                return tuple2Iterator.hasNext();
                            }

                            @Override
                            public T next()
                            {
                                return tuple2Iterator.next()._2();
                            }

                            @Override
                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                }, true);
    }

    /**
     * Count without duplicates.
     *
     * @return the long
     */
    public long countWithoutDuplicates()
    {

        List collectedResult = this.rawSpatialRDD.collect();
        HashSet resultWithoutDuplicates = new HashSet();
        for (int i = 0; i < collectedResult.size(); i++) {
            resultWithoutDuplicates.add(collectedResult.get(i));
        }
        return resultWithoutDuplicates.size();
    }

    /**
     * Count without duplicates SPRDD.
     *
     * @return the long
     */
    public long countWithoutDuplicatesSPRDD()
    {
        JavaRDD cleanedRDD = this.spatialPartitionedRDD;
        List collectedResult = cleanedRDD.collect();
        HashSet resultWithoutDuplicates = new HashSet();
        for (int i = 0; i < collectedResult.size(); i++) {
            resultWithoutDuplicates.add(collectedResult.get(i));
        }
        return resultWithoutDuplicates.size();
    }

    /**
     * Builds the index.
     *
     * @param indexType the index type
     * @param buildIndexOnSpatialPartitionedRDD the build index on spatial partitioned RDD
     * @throws Exception the exception
     */
    public void buildIndex(final IndexType indexType, boolean buildIndexOnSpatialPartitionedRDD)
            throws Exception
    {
        if (buildIndexOnSpatialPartitionedRDD == false) {
            //This index is built on top of unpartitioned SRDD
            this.indexedRawRDD = this.rawSpatialRDD.mapPartitions(new IndexBuilder(indexType));
        }
        else {
            if (this.spatialPartitionedRDD == null) {
                throw new Exception("[AbstractSpatialRDD][buildIndex] spatialPartitionedRDD is null. Please do spatial partitioning before build index.");
            }
            this.indexedRDD = this.spatialPartitionedRDD.mapPartitions(new IndexBuilder(indexType));
        }
    }

    /**
     * Boundary.
     *
     * @return the envelope
     * @deprecated Call analyze() instead
     */
    public Envelope boundary()
    {
        this.analyze();
        return this.boundaryEnvelope;
    }

    /**
     * Gets the raw spatial RDD.
     *
     * @return the raw spatial RDD
     */
    public JavaRDD<T> getRawSpatialRDD()
    {
        return rawSpatialRDD;
    }

    /**
     * Sets the raw spatial RDD.
     *
     * @param rawSpatialRDD the new raw spatial RDD
     */
    public void setRawSpatialRDD(JavaRDD<T> rawSpatialRDD)
    {
        this.rawSpatialRDD = rawSpatialRDD;
    }

    /**
     * Analyze.
     *
     * @param newLevel the new level
     * @return true, if successful
     */
    public boolean analyze(StorageLevel newLevel)
    {
        this.rawSpatialRDD = this.rawSpatialRDD.persist(newLevel);
        this.analyze();
        return true;
    }

    /**
     * Analyze.
     *
     * @return true, if successful
     */
    public boolean analyze()
    {
        final Function2 combOp =
                new Function2<StatCalculator, StatCalculator, StatCalculator>()
                {
                    @Override
                    public StatCalculator call(StatCalculator agg1, StatCalculator agg2)
                            throws Exception
                    {
                        return StatCalculator.combine(agg1, agg2);
                    }
                };

        final Function2 seqOp = new Function2<StatCalculator, Geometry, StatCalculator>()
        {
            @Override
            public StatCalculator call(StatCalculator agg, Geometry object)
                    throws Exception
            {
                return StatCalculator.add(agg, object);
            }
        };

        StatCalculator agg = (StatCalculator) this.rawSpatialRDD.aggregate(null, seqOp, combOp);
        if (agg != null) {
            this.boundaryEnvelope = agg.getBoundary();
            this.approximateTotalCount = agg.getCount();
            this.avgMBRArea=agg.getAvgMbrArea();
            this.avgLenX=agg.getAvgLenX();
            this.avgLenY=agg.getAvgLenY();
        }
        else {
            this.boundaryEnvelope = null;
            this.approximateTotalCount = 0;
            this.avgMBRArea=0.0;
            this.avgLenX=0.0;
            this.avgLenY=0.0;
        }
        int[] l=new int[]{2,3,5,7,9,11};
        double sumBC2=0;
        int i=0;
        double[][] datapPointsBC0=new double[6][2];
        double[][] datapPointsBC2=new double[6][2];

        List <Tuple2> bc0Points=new ArrayList<Tuple2>();
        //JavaPairRDD<Coordinate, Integer> pairs = this.rawSpatialRDD.mapToPair(s -> new Tuple2( new Coordinate(Math.floor(s.getCentroid().getX()/l), Math.floor(s.getCentroid().getY()/l)) , 1));
        // pairing box counting results
        for (int len:l) {
            sumBC2=0;
            JavaPairRDD<Coordinate, Integer> pairs = this.rawSpatialRDD.mapToPair(s -> new Tuple2( new Tuple2(Math.floor(s.getCentroid().getX()/len), Math.floor(s.getCentroid().getY()/len)) , 1));
            // getting the final result by reduceBy
            JavaPairRDD<Coordinate, Integer> countsInEachCell = pairs.reduceByKey((a, b) -> a + b);
            for (Tuple2 a:countsInEachCell.collect()) {
                int val= (int) a._2;
                sumBC2=sumBC2+Math.pow(val,2);// getting the value of BC2 = summation of counts square in each cell.
            }
            bc0Points.add(new Tuple2<>(len,sumBC2));
            datapPointsBC0[i][0]=Math.log(len);
            datapPointsBC0[i][1]=Math.log(countsInEachCell.count());//storing points in 2D array for regression BC0.
            datapPointsBC2[i][0]=Math.log(len);
            datapPointsBC2[i][1]=Math.log(sumBC2);//storing points in 2D array for regression BC2.
            i++;
        }
        /*for (Tuple2 t:bc0Points) {
            System.out.println(t._1+" :"+t._2);
        }*/
        /*for (int j = 0; j < 6; j++) {
            System.out.println("BC0 points:");
            System.out.println(datapPointsBC0[j][0]+" "+datapPointsBC0[j][1]);
            System.out.println("BC2 points:");
            System.out.println(datapPointsBC2[j][0]+" "+datapPointsBC2[j][1]);
        }*/
        SimpleRegression regression = new SimpleRegression();
        regression.addData(datapPointsBC0);
        this.E_0=regression.getSlope();
        System.out.println("E0 Slope: "+ E_0);
        SimpleRegression regression2 = new SimpleRegression();
        regression2.addData(datapPointsBC2);
        this.E_2=regression2.getSlope();
        System.out.println("E2 Slope: "+ E_2);

        System.out.println("New Method:");
        double sumBC2f=0;
        int iff=0;
        double[][] datapPointsBC0f=new double[6][2];
        double[][] datapPointsBC2f=new double[6][2];
        JavaPairRDD<Tuple3<Double,Double,Double>, Integer> flatmaptopairresult = this.rawSpatialRDD.flatMapToPair(s -> {
            List<Tuple2<String, Integer>> resulttuple=new ArrayList<>();
            double[] le=new double[]{2,3,5,7,9,11};
            //Tuple3<Double,Double,Double> res;
            List<Tuple2<Tuple3<Double,Double,Double>, Integer>> resu=new ArrayList<>();
            for (double li:le) {
                //resulttuple.add(new Tuple2<>(tmp,1));
                resu.add(new Tuple2<Tuple3<Double,Double,Double>, Integer>(new Tuple3<>(Math.floor(s.getCentroid().getX()/li), Math.floor(s.getCentroid().getY()/li),li),1));

            }
            return resu.iterator();
        });
        // getting the reduced by result like this (x,y,len)-> count
        JavaPairRDD<Tuple3<Double,Double,Double>, Integer> countsInEachCell = flatmaptopairresult.reduceByKey((a, b) -> a + b);
        // Mapping the previous reduced by result to get the sum of bc0 for mapping to each length. Example: len-> count (Ex: 2->1)
        JavaPairRDD<Double, Double> BC0Map = countsInEachCell.mapToPair(s -> new Tuple2<Double,Double>((double) ((Tuple3) s._1)._3(),1.00));
        // Reduced the BC0Map to get the total count of each length. . Example: len-> count (Ex: 2->5)
        JavaPairRDD<Double, Double> BC0Value = BC0Map.reduceByKey((a, b) -> a + b);
        // Mapping the countsInEachCell reduced by result to get the square of count for bc2 for mapping to each length. Example: len-> count (Ex: 2->2^2=4)
        JavaPairRDD<Double, Double> BC2pair = countsInEachCell.mapToPair(s ->
        {
            int val=s._2;
            return new Tuple2<Double,Double>((double) ((Tuple3) s._1)._3(), (double) Math.pow(val,2));
        });
        // Reduced the BC2Map to get the total sum of each length. . Example: len-> count (Ex: 2->2^2+...+2^2=20)
        JavaPairRDD<Double, Double> BC2Value = BC2pair.reduceByKey((a, b) -> a + b);
        int index=0;
        //Creating the data points for BC_0
        BC0Value=BC0Value.sortByKey();
        for (Tuple2 a:BC0Value.collect()) {
            double length=(double)a._1;
            double val=(double)a._2;
            datapPointsBC0f[index][0]=Math.log((double) a._1);
            datapPointsBC0f[index][1]=Math.log(val);
            index++;
        }
        index=0;
        //Creating the data points for BC_2
        BC2Value=BC2Value.sortByKey();
        for (Tuple2 a:BC2Value.collect()) {
            double length=(double) a._1;
            double val=(double)a._2;
            datapPointsBC2f[index][0]=Math.log(length);
            datapPointsBC2f[index][1]=Math.log(val);
            index++;
        }

        SimpleRegression reg_E0 = new SimpleRegression();
        reg_E0.addData(datapPointsBC0f);
        this.E_0=reg_E0.getSlope();
        System.out.println("E0 Slope: "+ E_0);
        SimpleRegression reg_E2 = new SimpleRegression();
        reg_E2.addData(datapPointsBC2f);
        this.E_2=reg_E2.getSlope();
        System.out.println("E2 Slope: "+ E_2);




        /*JavaPairRDD<Coordinate, Integer> pairs = this.rawSpatialRDD.mapToPair(s -> new Tuple2( new Tuple2(Math.floor(s.getCentroid().getX()/l), Math.floor(s.getCentroid().getY()/l)) , 1));
        // getting the final result by reduceBy
        JavaPairRDD<Coordinate, Integer> countsInEachCell = pairs.reduceByKey((a, b) -> a + b);
        for (Tuple2 a:countsInEachCell.collect()) {
            System.out.println( a._1+"  : "+ a._2);
        }*/

        return true;
    }

    public boolean analyze(Envelope datasetBoundary, Integer approximateTotalCount,Double avgMBRArea,Double avgLenX,Double avgLenY)
    {
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
        this.avgMBRArea=avgMBRArea;
        this.avgLenX=avgLenX;
        this.avgLenY=avgLenY;

        return true;
    }

    /**
     * Save as WKB.
     *
     * @param outputLocation the output location
     */
    public void saveAsWKB(String outputLocation)
    {
        if (this.rawSpatialRDD == null) {
            throw new NullArgumentException("save as WKB cannot operate on null RDD");
        }
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, String>()
        {
            @Override
            public Iterator<String> call(Iterator<T> iterator)
                    throws Exception
            {
                WKBWriter writer = new WKBWriter(3, true);
                ArrayList<String> wkbs = new ArrayList<>();

                while (iterator.hasNext()) {
                    Geometry spatialObject = iterator.next();
                    String wkb = WKBWriter.toHex(writer.write(spatialObject));

                    if (spatialObject.getUserData() != null) {
                        wkbs.add(wkb + "\t" + spatialObject.getUserData());
                    }
                    else {
                        wkbs.add(wkb);
                    }
                }
                return wkbs.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }

    /**
     * Save as WKT
     */
    public void saveAsWKT(String outputLocation)
    {
        if (this.rawSpatialRDD == null) {
            throw new NullArgumentException("save as WKT cannot operate on null RDD");
        }
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, String>()
        {
            @Override
            public Iterator<String> call(Iterator<T> iterator)
                    throws Exception
            {
                WKTWriter writer = new WKTWriter(3);
                ArrayList<String> wkts = new ArrayList<>();

                while (iterator.hasNext()) {
                    Geometry spatialObject = iterator.next();
                    String wkt = writer.write(spatialObject);

                    if (spatialObject.getUserData() != null) {
                        wkts.add(wkt + "\t" + spatialObject.getUserData());
                    }
                    else {
                        wkts.add(wkt);
                    }
                }
                return wkts.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }

    /**
     * Save as geo JSON.
     *
     * @param outputLocation the output location
     */
    public void saveAsGeoJSON(String outputLocation)
    {
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, String>()
        {
            @Override
            public Iterator<String> call(Iterator<T> iterator)
                    throws Exception
            {
                ArrayList<String> result = new ArrayList();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (iterator.hasNext()) {
                    Geometry spatialObject = iterator.next();
                    Feature jsonFeature;
                    if (spatialObject.getUserData() != null) {
                        Map<String, Object> userData = new HashMap<String, Object>();
                        userData.put("UserData", spatialObject.getUserData());
                        jsonFeature = new Feature(writer.write(spatialObject), userData);
                    }
                    else {
                        jsonFeature = new Feature(writer.write(spatialObject), null);
                    }
                    String jsonstring = jsonFeature.toString();
                    result.add(jsonstring);
                }
                return result.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }

    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    @Deprecated
    public RectangleRDD MinimumBoundingRectangle()
    {
        JavaRDD<Polygon> rectangleRDD = this.rawSpatialRDD.map(new Function<T, Polygon>()
        {
            public Polygon call(T spatialObject)
            {
                Double x1, x2, y1, y2;
                LinearRing linear;
                Coordinate[] coordinates = new Coordinate[5];
                GeometryFactory fact = new GeometryFactory();
                final Envelope envelope = spatialObject.getEnvelopeInternal();
                x1 = envelope.getMinX();
                x2 = envelope.getMaxX();
                y1 = envelope.getMinY();
                y2 = envelope.getMaxY();
                coordinates[0] = new Coordinate(x1, y1);
                coordinates[1] = new Coordinate(x1, y2);
                coordinates[2] = new Coordinate(x2, y2);
                coordinates[3] = new Coordinate(x2, y1);
                coordinates[4] = coordinates[0];
                linear = fact.createLinearRing(coordinates);
                Polygon polygonObject = new Polygon(linear, null, fact);
                return polygonObject;
            }
        });
        return new RectangleRDD(rectangleRDD);
    }

    /**
     * Gets the CR stransformation.
     *
     * @return the CR stransformation
     */
    public boolean getCRStransformation()
    {
        return CRStransformation;
    }

    /**
     * Gets the source epsg code.
     *
     * @return the source epsg code
     */
    public String getSourceEpsgCode()
    {
        return sourceEpsgCode;
    }

    /**
     * Gets the target epgsg code.
     *
     * @return the target epgsg code
     */
    public String getTargetEpgsgCode()
    {
        return targetEpgsgCode;
    }

    public void flipCoordinates() {
        this.rawSpatialRDD = this.rawSpatialRDD.map(f -> {
            GeomUtils.flipCoordinates(f);
            return f;
        });
    }
}
