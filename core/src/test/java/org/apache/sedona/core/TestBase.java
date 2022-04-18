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

package org.apache.sedona.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.List;

public class TestBase
{
    protected static SparkConf conf;
    protected static JavaSparkContext sc;

    protected static void initialize(final String testSuiteName)
    {
        conf = new SparkConf().setAppName(testSuiteName).setMaster("local[2]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    public static List<Geometry> createPolygonOverlapping(int size) {
        GeometryFactory geometryFactory = new GeometryFactory();
        List<Geometry> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create polygons each of which only has 1 match in points
            // Each polygon is an envelope like (-0.5, -0.5, 0.5, 0.5)
            double minX = i - 1;
            double minY = 0;
            double maxX = i + 1;
            double maxY = 1;
            Coordinate[] coordinates = new Coordinate[5];
            coordinates[0] = new Coordinate(minX, minY);
            coordinates[1] = new Coordinate(minX, maxY);
            coordinates[2] = new Coordinate(maxX, maxY);
            coordinates[3] = new Coordinate(maxX, minY);
            coordinates[4] = coordinates[0];
            data.add(geometryFactory.createPolygon(coordinates));
        }
        return data;
    }
}

