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

package org.apache.sedona.core.spatialPartitioning;

import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * see https://en.wikipedia.org/wiki/K-D-B-tree
 */
public class KDB extends PartitioningUtils
        implements Serializable
{

    private final int maxItemsPerNode;
    private final int maxLevels;
    private final Envelope extent;
    private final int level;
    private final List<Envelope> items = new ArrayList<>();
    private KDB[] children;
    private int leafId = 0;

    public KDB(int maxItemsPerNode, int maxLevels, Envelope extent)
    {
        this(maxItemsPerNode, maxLevels, 0, extent);
    }

    private KDB(int maxItemsPerNode, int maxLevels, int level, Envelope extent)
    {
        this.maxItemsPerNode = maxItemsPerNode;
        this.maxLevels = maxLevels;
        this.level = level;
        this.extent = extent;
    }

    public int getItemCount()
    {
        return items.size();
    }

    public boolean isLeaf()
    {
        return children == null;
    }

    public int getLeafId()
    {
        if (!isLeaf()) {
            throw new IllegalStateException();
        }

        return leafId;
    }

    public Envelope getExtent()
    {
        return extent;
    }

    public void insert(Envelope envelope)
    {
        if (items.size() < maxItemsPerNode || level >= maxLevels) {
            items.add(envelope);
        }
        else {
            if (children == null) {
                // Split over longer side
                boolean splitX = extent.getWidth() > extent.getHeight();
                boolean ok = split(splitX);
                if (!ok) {
                    // Try spitting by the other side
                    ok = split(!splitX);
                }

                if (!ok) {
                    // This could happen if all envelopes are the same.
                    items.add(envelope);
                    return;
                }
            }

            for (KDB child : children) {
                if (child.extent.contains(envelope.getMinX(), envelope.getMinY())) {
                    child.insert(envelope);
                    break;
                }
            }
        }
    }

    public void dropElements()
    {
        traverse(new Visitor()
        {
            @Override
            public boolean visit(KDB tree)
            {
                tree.items.clear();
                return true;
            }
        });
    }

    public List<KDB> findLeafNodes(final Envelope envelope)
    {
        final List<KDB> matches = new ArrayList<>();
        traverse(new Visitor()
        {
            @Override
            public boolean visit(KDB tree)
            {
                if (!disjoint(tree.getExtent(), envelope)) {
                    if (tree.isLeaf()) {
                        matches.add(tree);
                    }
                    return true;
                }
                else {
                    return false;
                }
            }
        });

        return matches;
    }

    private boolean disjoint(Envelope r1, Envelope r2)
    {
        return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    public void traverse(Visitor visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (children != null) {
            for (KDB child : children) {
                child.traverse(visitor);
            }
        }
    }

    public void assignLeafIds()
    {
        traverse(new Visitor()
        {
            int id = 0;

            @Override
            public boolean visit(KDB tree)
            {
                if (tree.isLeaf()) {
                    tree.leafId = id;
                    id++;
                }
                return true;
            }
        });
    }

    private boolean split(boolean splitX)
    {
        final Comparator<Envelope> comparator = splitX ? new XComparator() : new YComparator();
        Collections.sort(items, comparator);

        final Envelope[] splits;
        final Splitter splitter;
        Envelope middleItem = items.get((int) Math.floor(items.size() / 2));
        if (splitX) {
            double x = middleItem.getMinX();
            if (x > extent.getMinX() && x < extent.getMaxX()) {
                splits = splitAtX(extent, x);
                splitter = new XSplitter(x);
            }
            else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        }
        else {
            double y = middleItem.getMinY();
            if (y > extent.getMinY() && y < extent.getMaxY()) {
                splits = splitAtY(extent, y);
                splitter = new YSplitter(y);
            }
            else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        }

        children = new KDB[2];
        children[0] = new KDB(maxItemsPerNode, maxLevels, level + 1, splits[0]);
        children[1] = new KDB(maxItemsPerNode, maxLevels, level + 1, splits[1]);

        // Move items
        splitItems(splitter);
        return true;
    }

    private void splitItems(Splitter splitter)
    {
        for (Envelope item : items) {
            children[splitter.split(item) ? 0 : 1].insert(item);
        }
    }

    private Envelope[] splitAtX(Envelope envelope, double x)
    {
        assert (envelope.getMinX() < x);
        assert (envelope.getMaxX() > x);
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getMinX(), x, envelope.getMinY(), envelope.getMaxY());
        splits[1] = new Envelope(x, envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
        return splits;
    }

    private Envelope[] splitAtY(Envelope envelope, double y)
    {
        assert (envelope.getMinY() < y);
        assert (envelope.getMaxY() > y);
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), y);
        splits[1] = new Envelope(envelope.getMinX(), envelope.getMaxX(), y, envelope.getMaxY());
        return splits;
    }

    @Override
    public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry geometry) {
        Objects.requireNonNull(geometry, "spatialObject");

        final Envelope envelope = geometry.getEnvelopeInternal();

        final List<KDB> matchedPartitions = findLeafNodes(envelope);

        final Point point = geometry instanceof Point ? (Point) geometry : null;

        final Set<Tuple2<Integer, Geometry>> result = new HashSet<>();
        for (KDB leaf : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(leaf.getExtent())).contains(point)) {
                continue;
            }

            result.add(new Tuple2(leaf.getLeafId(), geometry));
        }

        return result.iterator();
    }
    //aabir
    public Iterator<Tuple2<Integer, Tuple2 <Geometry,Short>>> placeObjectN(Geometry geometry) {
        Objects.requireNonNull(geometry, "spatialObject");

        final Envelope envelope = geometry.getEnvelopeInternal();

        final List<KDB> matchedPartitions = findLeafNodes(envelope);

        final Point point = geometry instanceof Point ? (Point) geometry : null;

        final Set<Tuple2<Integer, Tuple2 <Geometry,Short>>> result = new HashSet<>();
        for (KDB leaf : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(leaf.getExtent())).contains(point)) {
                continue;
            }
            Envelope obj=geometry.getEnvelopeInternal();
            if(obj.getMinX() >= leaf.getExtent().getMinX() &&
                    obj.getMinX() < leaf.getExtent().getMaxX() &&
                    obj.getMinY() >= leaf.getExtent().getMinY() &&
                    obj.getMinY() < leaf.getExtent().getMaxY()){
                System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+ " Grid:  "+ leaf.getExtent() +"  Class A");
                result.add(new Tuple2(leaf.getLeafId(), new Tuple2(geometry,(short)1)));
            }
            else if((obj.getMinX() >= leaf.getExtent().getMinX() &&
                    obj.getMinX() < leaf.getExtent().getMaxX()) &&
                    (obj.getMinY() > leaf.getExtent().getMinY() ||
                            obj.getMinY() < leaf.getExtent().getMaxY())){
                System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+ " Grid:  "+ leaf.getExtent() +"  Class B");
                result.add(new Tuple2(leaf.getLeafId(), new Tuple2(geometry,(short)2)));
            }
            else if((obj.getMinX() > leaf.getExtent().getMinX() ||
                    obj.getMinX() < leaf.getExtent().getMaxX()) &&
                    (obj.getMinY() >= leaf.getExtent().getMinY() &&
                    obj.getMinY() < leaf.getExtent().getMaxY())){
                System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+" Grid:  "+ leaf.getExtent() +"  Class C");
                result.add(new Tuple2(leaf.getLeafId(), new Tuple2(geometry,(short)3)));
            }
            else if((obj.getMinX() > leaf.getExtent().getMinX() ||
                    obj.getMinX() < leaf.getExtent().getMaxX()) &&
                    (obj.getMinY() > leaf.getExtent().getMinY() ||
                            obj.getMinY() < leaf.getExtent().getMaxY())){
                System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+ " Grid:  "+ leaf.getExtent() +"  Class D");
                result.add(new Tuple2(leaf.getLeafId(), new Tuple2(geometry,(short)4)));
            }

            //System.out.println("Grid X range: " +  leaf.getExtent().getMinX() + " "+leaf.getExtent().getMaxX() +"    Y Range: "+ leaf.getExtent().getMinY() + " "+leaf.getExtent().getMaxY() );
            //System.out.println("Object  top left corner: " +  geometry.getEnvelopeInternal().getMinX() + "  "+ geometry.getEnvelopeInternal().getMinY() );

            //result.add(new Tuple2(leaf.getLeafId(), geometry));
        }

        return result.iterator();
    }
    public Iterator<Tuple2<Integer, Tuple3<Geometry,Short,Long>>> placeObjectN2(Tuple2<Geometry,Long> geometry) {
        Objects.requireNonNull(geometry._1, "spatialObject");

        final Envelope envelope = geometry._1.getEnvelopeInternal();

        final List<KDB> matchedPartitions = findLeafNodes(envelope);

        final Point point = geometry._1 instanceof Point ? (Point) geometry._1 : null;

        final Set<Tuple2<Integer, Tuple3 <Geometry,Short,Long>>> result = new HashSet<>();
        for (KDB leaf : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(leaf.getExtent())).contains(point)) {
                continue;
            }
            Envelope obj=geometry._1.getEnvelopeInternal();
            //System.out.println("Id "+geometry._2+" Obj: ("+obj.getMinX()+", "+obj.getMinY() + ") Grid:"+leaf.getExtent());
            //System.out.println(obj+"  "+leaf.getExtent()+" "+ leaf.getExtent().covers(obj));
            if((obj.getMinX() >= leaf.getExtent().getMinX() &&
                    obj.getMinX() < leaf.getExtent().getMaxX()) &&
                    (obj.getMinY() >= leaf.getExtent().getMinY() &&
                            obj.getMinY() < leaf.getExtent().getMaxY())){

                /*if(leaf.getExtent().covers(obj) && !coversOnBoundary(leaf.getExtent(),obj)){
                        result.add(new Tuple2(leaf.getLeafId(), new Tuple3(geometry._1,(short)1,geometry._2)));
                        System.out.println(geometry._2+" "+obj+"  "+leaf.getExtent()+" "+ coversOnBoundary(leaf.getExtent(),obj)+" 1 check");
                }else {
                    result.add(new Tuple2(leaf.getLeafId(), new Tuple3(geometry._1,(short)5,geometry._2)));
                    System.out.println(geometry._2+" "+obj+"  "+leaf.getExtent()+" "+ coversOnBoundary(leaf.getExtent(),obj)+" 5");
                    //System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+" Obj: "+obj + " Grid:  "+ leaf.getExtent() +"  Class 5");
                }*/
                if(fullyCoveredByFst(leaf.getExtent(),obj)){
                    result.add(new Tuple2(leaf.getLeafId(), new Tuple3(geometry._1,(short)1,geometry._2)));
                    //System.out.println(geometry._2+" "+obj+"  "+leaf.getExtent()+" "+ coversOnBoundary(leaf.getExtent(),obj)+" 1 check");
                }else {
                    result.add(new Tuple2(leaf.getLeafId(), new Tuple3(geometry._1,(short)5,geometry._2)));
                    //System.out.println(geometry._2+" "+obj+"  "+leaf.getExtent()+" "+ coversOnBoundary(leaf.getExtent(),obj)+" 5");
                    //System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+" Obj: "+obj + " Grid:  "+ leaf.getExtent() +"  Class 5");
                }

            }
            else if((obj.getMinX() >= leaf.getExtent().getMinX() &&
                    obj.getMinX() < leaf.getExtent().getMaxX()) &&
                    !(obj.getMinY() >= leaf.getExtent().getMinY() &&
                            obj.getMinY() < leaf.getExtent().getMaxY())){
                //System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+ " Grid:  "+ leaf.getExtent() +"  Class B");
                result.add(new Tuple2(leaf.getLeafId(), new Tuple3(null,(short)2,geometry._2)));
                //System.out.println(geometry._2+" "+obj+"  "+leaf.getExtent()+" "+ leaf.getExtent().intersects(obj)+" 2");

            }
            else if(!(obj.getMinX() >= leaf.getExtent().getMinX() &&
                    obj.getMinX() < leaf.getExtent().getMaxX()) &&
                    (obj.getMinY() >= leaf.getExtent().getMinY() &&
                            obj.getMinY() < leaf.getExtent().getMaxY())){
                //System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+" Grid:  "+ leaf.getExtent() +"  Class C");
                result.add(new Tuple2(leaf.getLeafId(), new Tuple3(null,(short)3,geometry._2)));
                //System.out.println(geometry._2+" "+obj+"  "+leaf.getExtent()+" "+ coversOnBoundary(leaf.getExtent(),obj)+" 3");

            }
            else if(!(obj.getMinX() >= leaf.getExtent().getMinX() &&
                    obj.getMinX() < leaf.getExtent().getMaxX()) &&
                    !(obj.getMinY() >= leaf.getExtent().getMinY() &&
                            obj.getMinY() < leaf.getExtent().getMaxY())){
                //System.out.println("Object "+obj.getMinX() +" "+obj.getMinY()+ " Grid:  "+ leaf.getExtent() +"  Class D");
                result.add(new Tuple2(leaf.getLeafId(), new Tuple3(null,(short)4,geometry._2)));
                //System.out.println(geometry._2+" "+obj+"  "+leaf.getExtent()+" "+ leaf.getExtent().contains(obj)+" 4");

            }

            //System.out.println("Grid X range: " +  leaf.getExtent().getMinX() + " "+leaf.getExtent().getMaxX() +"    Y Range: "+ leaf.getExtent().getMinY() + " "+leaf.getExtent().getMaxY() );
            //System.out.println("Object  top left corner: " +  geometry.getEnvelopeInternal().getMinX() + "  "+ geometry.getEnvelopeInternal().getMinY() );

            //result.add(new Tuple2(leaf.getLeafId(), geometry));
        }

        return result.iterator();
    }

    @Override
    public Set<Integer> getKeys(Geometry geometry) {
        Objects.requireNonNull(geometry, "spatialObject");

        final Envelope envelope = geometry.getEnvelopeInternal();

        final List<KDB> matchedPartitions = findLeafNodes(envelope);

        final Point point = geometry instanceof Point ? (Point) geometry : null;

        final Set<Integer> result = new HashSet<>();
        for (KDB leaf : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(leaf.getExtent())).contains(point)) {
                continue;
            }

            result.add(leaf.getLeafId());
        }
        return result;
    }
    public static boolean fullyCoveredByFst(Envelope e1, Envelope e2) {
        if (e1.isNull() || e2.isNull()) { return false; }
        return e2.getMinX() > e1.getMinX() &&
                e2.getMaxX() < e1.getMaxX() &&
                e2.getMinY() > e1.getMinY() &&
                e2.getMaxY() < e1.getMaxY();
    }
    public static boolean coversOnBoundary(Envelope e1, Envelope e2) {
        if (e1.isNull() || e2.isNull()) { return false; }
        return e2.getMinX() == e1.getMinX() ||
                e2.getMaxX() == e1.getMaxX() ||
                e2.getMinY() == e1.getMinY() ||
                e2.getMaxY() == e1.getMaxY();
    }
    @Override
    public List<Envelope> fetchLeafZones() {
        final List<Envelope> leafs = new ArrayList<>();
        this.traverse(new KDB.Visitor()
        {
            @Override
            public boolean visit(KDB tree)
            {
                if (tree.isLeaf()) {
                    leafs.add(tree.getExtent());
                }
                return true;
            }
        });
        return leafs;
    }

    public interface Visitor
    {
        /**
         * Visits a single node of the tree
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(KDB tree);
    }

    private interface Splitter
    {
        /**
         * @return true if the specified envelope belongs to the lower split
         */
        boolean split(Envelope envelope);
    }

    private static final class XComparator
            implements Comparator<Envelope>
    {
        @Override
        public int compare(Envelope o1, Envelope o2)
        {
            double deltaX = o1.getMinX() - o2.getMinX();
            return (int) Math.signum(deltaX != 0 ? deltaX : o1.getMinY() - o2.getMinY());
        }
    }

    private static final class YComparator
            implements Comparator<Envelope>
    {
        @Override
        public int compare(Envelope o1, Envelope o2)
        {
            double deltaY = o1.getMinY() - o2.getMinY();
            return (int) Math.signum(deltaY != 0 ? deltaY : o1.getMinX() - o2.getMinX());
        }
    }

    private static final class XSplitter
            implements Splitter
    {
        private final double x;

        private XSplitter(double x)
        {
            this.x = x;
        }

        @Override
        public boolean split(Envelope envelope)
        {
            return envelope.getMinX() <= x;
        }
    }

    private static final class YSplitter
            implements Splitter
    {
        private final double y;

        private YSplitter(double y)
        {
            this.y = y;
        }

        @Override
        public boolean split(Envelope envelope)
        {
            return envelope.getMinY() <= y;
        }
    }
}
