package com.studql.rtree.spark.datamapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.studql.shape.Point;
import com.studql.utils.Benchmark;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class PointDataMapper extends DataMapper<Point> implements Serializable {

	private static final long serialVersionUID = -1631764482513057782L;

	public PointDataMapper(String delimiter, int[] pointPositionInLine, Pair<float[], float[]> rangeInterpolators,
			boolean hasHeader) {
		this.delimiter = delimiter;
		this.pointPositionInLine = pointPositionInLine;
		this.rangeInterpolators = rangeInterpolators;
		this.hasHeader = hasHeader;
	}

	private Iterator<Record<Point>> selectIterator(List<Record<Point>> result, float[] xRange, float[] yRange,
			float minX, float maxX, float minY, float maxY) {
		if (xRange != null && yRange != null) {
			List<Record<Point>> interpolatedResult = new ArrayList<Record<Point>>();
			float[] initialXRange = new float[] { minX, maxX };
			float[] initialYRange = new float[] { minY, maxY };
			int i = 0;
			for (Record<Point> p : result) {
				float x = p.getValue().getX(), y = p.getValue().getY();
				float interpolatedX = Benchmark.interpolatePoint(x, initialXRange, xRange);
				float interpolatedY = Benchmark.interpolatePoint(y, initialYRange, yRange);
				interpolatedResult
						.add(new Record<Point>(new Point(interpolatedX, interpolatedY), Integer.toString(i++)));
			}
			return interpolatedResult.iterator();
		}
		return result.iterator();
	}

	@Override
	public Iterator<Record<Point>> call(Iterator<String> dataIterator) throws Exception {
		List<Record<Point>> result = new ArrayList<Record<Point>>();
		// getting expected points position in a row
		int posX = pointPositionInLine[0], posY = pointPositionInLine[1];
		// getting range interpolators for Points
		float[] xRange = rangeInterpolators.getFirst(), yRange = rangeInterpolators.getSecond();
		// reading through file, recording min and max coordinate range
		float minX = Float.MAX_VALUE, maxX = -Float.MAX_VALUE, minY = Float.MAX_VALUE, maxY = -Float.MAX_VALUE;
		int i = 0;
		while (dataIterator.hasNext()) {
			String line = dataIterator.next();
			// skipping header
			if (i == 0 && hasHeader && dataIterator.hasNext())
				line = dataIterator.next();
			String splittedRow[] = line.split(delimiter);
			// the splitted row has at least two entries
			assert splittedRow.length >= 2;
			float x = Float.parseFloat(splittedRow[posX]), y = Float.parseFloat(splittedRow[posY]);
			// updating coordinates limits
			minX = Math.min(minX, x);
			maxX = Math.max(maxX, x);
			minY = Math.min(minY, y);
			maxY = Math.max(maxY, y);
			result.add(new Record<Point>(new Point(x, y), Integer.toString(i++)));
		}
		// interpolating results if needed
		return this.selectIterator(result, xRange, yRange, minX, maxX, minY, maxY);
	}

}
