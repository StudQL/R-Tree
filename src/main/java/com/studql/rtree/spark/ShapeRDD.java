package com.studql.rtree.spark;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.random.SamplingUtils;

import com.studql.rtree.Rtree;
import com.studql.rtree.node.NodeSplitter;
import com.studql.rtree.spark.datamapper.DataMapper;
import com.studql.rtree.spark.queries.ExactQueryFilter;
import com.studql.rtree.spark.queries.ExactQueryFilterWithIndex;
import com.studql.rtree.spark.queries.KnnDistanceComparator;
import com.studql.rtree.spark.queries.KnnQueryFilter;
import com.studql.rtree.spark.queries.KnnQueryFilterWithIndex;
import com.studql.rtree.spark.queries.RangeQueryFilter;
import com.studql.rtree.spark.queries.RangeQueryFilterWithIndex;
import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Pair;
import com.studql.utils.Record;
import com.studql.utils.Visualizer;

import scala.Tuple2;

public class ShapeRDD<T extends Boundable> implements Serializable {

	private static final long serialVersionUID = 1568538010728137513L;
	protected JavaRDD<Record<T>> initialRDD;
	protected JavaRDD<Rtree<T>> indexedInitialRDD;
	protected JavaRDD<Record<T>> spatialRDD;
	protected JavaRDD<Rtree<T>> indexedSpatialRDD;
	protected long numRecords;
	protected Rectangle datasetMbr;
	protected List<Rectangle> partitionGrids;

	public ShapeRDD(JavaSparkContext sc, String inputLocation, DataMapper<T> dataMapper, int min_num_records,
			int max_num_records, NodeSplitter<T> splitter, int num_partitions) {
		this.initialRDD = createInitialRDD(sc, inputLocation, dataMapper, num_partitions);
		this.initialRDD.persist(StorageLevel.DISK_ONLY());
		this.indexedInitialRDD = createIndexedInitialRDD(min_num_records, max_num_records, splitter);
		this.indexedInitialRDD.persist(StorageLevel.DISK_ONLY());
		this.spatialRDD = createPartitionedRDD(splitter);
		this.spatialRDD.persist(StorageLevel.DISK_ONLY());
		this.indexedSpatialRDD = createIndexedPartitionedRDD(min_num_records, max_num_records, splitter);
		this.indexedSpatialRDD.persist(StorageLevel.DISK_ONLY());
//		this.visualizeGrids(partitionGrids);
	}

	private JavaRDD<Record<T>> createInitialRDD(JavaSparkContext sc, String inputLocation, DataMapper<T> dataMapper,
			int num_partitions) {
		JavaRDD<String> dataLines = sc.textFile(inputLocation, num_partitions);
		// loading RDD with respect to input data
		return dataLines.mapPartitions(dataMapper);
	}

	private JavaRDD<Rtree<T>> createIndexedInitialRDD(int min_num_records, int max_num_records,
			NodeSplitter<T> splitter) {
		// building one rtree for each partition
		RtreeIndexBuilder<T> indexBuilder = new RtreeIndexBuilder<T>(min_num_records, max_num_records, splitter);
		JavaRDD<Rtree<T>> res = this.initialRDD.mapPartitions(indexBuilder);
		return res;
	}

	private JavaRDD<Rtree<T>> createIndexedPartitionedRDD(int min_num_records, int max_num_records,
			NodeSplitter<T> splitter) {
		RtreeIndexBuilder<T> indexBuilder = new RtreeIndexBuilder<T>(min_num_records, max_num_records, splitter);
		// building one rtree for each partition
		JavaRDD<Rtree<T>> res = this.spatialRDD.mapPartitions(indexBuilder);
//		int i = 0;
//		for (Rtree<T> tree : res.collect()) {
//			try {
//				new Visualizer<T>().createVisualization(tree,
//						new File("C:\\Users\\alzajac\\Downloads\\test" + i + ".png"));
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			i++;
//		}
		return res;
	}

	private JavaRDD<Record<T>> createPartitionedRDD(NodeSplitter<T> splitter) {
		this.computeDatasetLimits();
		// take 1% for sampling
		long num_samples = this.numRecords / 100;
		// compute approximation fraction from sample
		double sample_fraction = SamplingUtils.computeFractionForSampleSize((int) num_samples, this.numRecords, false);
		List<Record<T>> sampledRecords = this.initialRDD.sample(false, sample_fraction).collect();
		// partition with Rtree index
		int numPartitions = this.initialRDD.rdd().partitions().length;
		Function<Integer, Integer> computeMinRecords = num -> num / 2;
		SpatialPartitioner<T> partitioner = new SpatialPartitioner<T>(sampledRecords, numPartitions, splitter,
				computeMinRecords);
		// repartition the RDD by spatial grids
		this.partitionGrids = partitioner.getSpaceGrids();
		return rePartition(new GridPartitioner<T>(partitionGrids));
	}

	private JavaRDD<Record<T>> rePartition(GridPartitioner<T> gridPartitioner) {
		// re-assign each record to its bounding grid
		// split them by id of the grid partition
		// preserve partitioning with returning the Value in (key, value) pairs
		return this.initialRDD.flatMapToPair(getGridAssignFunction(gridPartitioner)).partitionBy(gridPartitioner)
				.mapPartitions(getGridPairFunction(), true);
	}

	private PairFlatMapFunction<Record<T>, Integer, Record<T>> getGridAssignFunction(
			GridPartitioner<T> gridPartitioner) {
		return new PairFlatMapFunction<Record<T>, Integer, Record<T>>() {

			private static final long serialVersionUID = 3671975693640688180L;

			@Override
			public Iterator<Tuple2<Integer, Record<T>>> call(Record<T> record) throws Exception {
				return gridPartitioner.assignRecord(record);
			}
		};
	}

	private FlatMapFunction<Iterator<Tuple2<Integer, Record<T>>>, Record<T>> getGridPairFunction() {
		return new FlatMapFunction<Iterator<Tuple2<Integer, Record<T>>>, Record<T>>() {

			private static final long serialVersionUID = -6901920611972673524L;

			@Override
			public Iterator<Record<T>> call(final Iterator<Tuple2<Integer, Record<T>>> idRecordIterator)
					throws Exception {
				return new Iterator<Record<T>>() {
					@Override
					public boolean hasNext() {
						return idRecordIterator.hasNext();
					}

					@Override
					public Record<T> next() {
						return idRecordIterator.next()._2();
					}
				};
			}
		};
	}

	private void computeDatasetLimits() {
		// compute limits (count of records and MBR)
		DatasetLimits<T> limits = this.initialRDD.aggregate(null, DatasetLimits.getAddFunction(),
				DatasetLimits.getMergeFunction());
		this.datasetMbr = limits.getMbr();
		this.numRecords = limits.getRecordCount();
	}

	private void visualizeGrids(List<Rectangle> rectangles) {
		Visualizer<T> v = new Visualizer<T>();
		try {
			v.createGridVisualization(initialRDD.collect(), rectangles, datasetMbr,
					new File("C:\\Users\\alzajac\\Downloads\\test.png"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public JavaRDD<Record<T>> exactQuery(Record<T> searchRecord, boolean withIndex, boolean withInitialData) {
		if (withIndex) {
			if (withInitialData)
				return indexedInitialRDD.mapPartitions(new ExactQueryFilterWithIndex<T>(searchRecord));
			return indexedSpatialRDD.mapPartitions(new ExactQueryFilterWithIndex<T>(searchRecord));
		} else if (withInitialData)
			return initialRDD.filter(new ExactQueryFilter<T>(searchRecord));
		return spatialRDD.filter(new ExactQueryFilter<T>(searchRecord));
	}

	public JavaRDD<Record<T>> rangeQuery(Rectangle searchRange, boolean withIndex, boolean withInitialData) {
		if (withIndex) {
			if (withInitialData)
				return indexedInitialRDD.mapPartitions(new RangeQueryFilterWithIndex<T>(searchRange));
			return indexedSpatialRDD.mapPartitions(new RangeQueryFilterWithIndex<T>(searchRange));
		} else if (withInitialData)
			return initialRDD.filter(new RangeQueryFilter<T>(searchRange));
		return spatialRDD.filter(new RangeQueryFilter<T>(searchRange));
	}

	public List<Pair<Record<T>, Float>> knnQuery(Record<T> centerRecord, int k, boolean withIndex,
			boolean withInitialData) {
		JavaRDD<Pair<Record<T>, Float>> partitionResult;
		if (withIndex) {
			if (withInitialData)
				partitionResult = indexedInitialRDD.mapPartitions(new KnnQueryFilterWithIndex<T>(k, centerRecord));
			else
				partitionResult = indexedSpatialRDD.mapPartitions(new KnnQueryFilterWithIndex<T>(k, centerRecord));
		} else if (withInitialData)
			partitionResult = initialRDD.mapPartitions(new KnnQueryFilter<T>(k, centerRecord));
		else
			partitionResult = spatialRDD.mapPartitions(new KnnQueryFilter<T>(k, centerRecord));
		// we need to take the top k from each collected partition to be correct
		return partitionResult.takeOrdered(k, new KnnDistanceComparator<T>());
	}

	public JavaRDD<Rtree<T>> getIndexedInitialRDD() {
		return indexedInitialRDD;
	}

	public JavaRDD<Rtree<T>> getIndexedSpatialRDD() {
		return indexedSpatialRDD;
	}

	public JavaRDD<Record<T>> getInitialRDD() {
		return initialRDD;
	}

	public JavaRDD<Record<T>> getSpatialRDD() {
		return spatialRDD;
	}
}
