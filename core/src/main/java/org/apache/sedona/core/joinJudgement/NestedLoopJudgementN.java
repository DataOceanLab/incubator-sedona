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

package org.apache.sedona.core.joinJudgement;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJudgementN<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<Tuple3<T,Short,Long>>, Iterator<Tuple3<U,Short,Long>>, Pair<Tuple2<Long,U>, Tuple2<Long,T>>>, Serializable
{
    private static final Logger log = LogManager.getLogger(NestedLoopJudgementN.class);

    /**
     * @see JudgementBase
     */
    public NestedLoopJudgementN(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams)
    {
        super(considerBoundaryIntersection, dedupParams);
    }

    @Override
    public Iterator<Pair<Tuple2<Long,U>, Tuple2<Long,T>>> call(Iterator<Tuple3<T,Short,Long>> iteratorObject, Iterator<Tuple3<U,Short,Long>> iteratorWindow)
            throws Exception
    {
        if (!iteratorObject.hasNext() || !iteratorWindow.hasNext()) {
            return Collections.emptyIterator();
        }

        initPartition();

        List<Pair<Tuple2<Long,U>, Tuple2<Long,T>>> result = new ArrayList<>();
        List<Tuple3<Long, T, Short>> queryObjects = new ArrayList<>();
        while (iteratorObject.hasNext()) {
            Tuple3<T,Short,Long> itr=iteratorObject.next();
            queryObjects.add(new Tuple3<Long,T,Short>(itr._3(),itr._1(),itr._2()));
        }
        while (iteratorWindow.hasNext()) {
            //U window = iteratorWindow.next()._1();
            Tuple3<U,Short,Long> window=iteratorWindow.next();
            for (int i = 0; i < queryObjects.size(); i++) {
                //T object = queryObjects.get(i)._2;
                Tuple3<Long,T,Short> object=queryObjects.get(i);
                //log.warn("Check "+window.toText()+" with "+object.toText());
                if (window._1() != null && object._2() !=null ) {
                    if(window._1().intersects(object._2())) {
                        result.add(Pair.of(new Tuple2<>(window._3(),window._1()), new Tuple2<>(object._1(),object._2())));
                    }
                }
                else if(window._1()==null || object._2()== null){
                    if(window._2()==1 || window._2()==5){
                        if(object._3()==1 || object._3()==5 ||object._3()==3 || object._3()==2 || object._3()==4){
                            result.add(Pair.of(new Tuple2<>(window._3(),window._1()), new Tuple2<>(object._1(),object._2())));
                        }
                    }
                    else if(window._2()==2){
                        if(object._3()==1 || object._3()==5 ||object._3()==3){
                            result.add(Pair.of(new Tuple2<>(window._3(),window._1()), new Tuple2<>(object._1(),object._2())));
                        }
                    }
                    else if(window._2()==3){
                        if(object._3()==1 || object._3()==5 ||object._3()==2){
                            result.add(Pair.of(new Tuple2<>(window._3(),window._1()), new Tuple2<>(object._1(),object._2())));
                        }
                    }
                    else if(window._2()==4){
                        if(object._3()==1 || object._3()==5){
                            result.add(Pair.of(new Tuple2<>(window._3(),window._1()), new Tuple2<>(object._1(),object._2())));
                        }
                    }
                }
            }
        }
        return result.iterator();
    }

}
