package com.studql.rtree.spark.datamapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.studql.shape.Point;
import com.studql.utils.Benchmark;
import com.studql.utils.Pair;

public class PointDataMapper extends DataMapper<Point> implements Serializable {

	private static final long serialVersionUID = -1631764482513057782L;

	public PointDataMapper(String delimiter, int[] pointPositionInLine, Pair<float[], float[]> rangeInterpolators,
			boolean hasHeader) {
		this.delimiter = delimiter;
		this.pointPositionInLine = pointPositionInLine;
		this.rangeInterpolators = rangeInterpolators;
		this.hasHeader = hasHeader;
	}

	private Iterator<Point> selectIterator(List<Point> result, float[] xRange, float[] yRange, float minX, float maxX,
			float minY, float maxY) {
		if (xRange != null && yRange != null) {
			List<Point> interpolatedResult = new ArrayList<Point>();
			float[] initialXRange = new float[] { minX, maxX };
			float[] initialYRange = new float[] { minY, maxY };
			for (Point p : result) {
				float x = p.getX(), y = p.getY();
				float interpolatedX = Benchmark.interpolatePoint(x, initialXRange, xRange);
				float interpolatedY = Benchmark.interpolatePoint(y, initialYRange, yRange);
				interpolatedResult.add(new Point(interpolatedX, interpolatedY));
			}
			return interpolatedResult.iterator();
		}
		return result.iterator();
	}

	@Override
	public Iterator<Point> call(Iterator<String> dataIterator) throws Exception {
		List<Point> result = new ArrayList<Point>();
		// getting expected points position in a row
		int posX = pointPositionInLine[0], posY = pointPositionInLine[1];
		// getting range interpolators for Points
		float[] xRange = rangeInterpolators.getFirst(), yRange = rangeInterpolators.getSecond();
		// reading through file, recording min and max coordinate range
		float i = 0, minX = Float.MAX_VALUE, maxX = -Float.MAX_VALUE, minY = Float.MAX_VALUE, maxY = -Float.MAX_VALUE;
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
			result.add(new Point(x, y));
			++i;
		}
		// interpolating results if needed
		return this.selectIterator(result, xRange, yRange, minX, maxX, minY, maxY);
	}

}
