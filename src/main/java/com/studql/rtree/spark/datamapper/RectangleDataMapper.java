package com.studql.rtree.spark.datamapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.studql.shape.Rectangle;
import com.studql.utils.Benchmark;
import com.studql.utils.Pair;

public class RectangleDataMapper extends DataMapper<Rectangle> implements Serializable {

	private static final long serialVersionUID = -1631764482513057782L;

	public RectangleDataMapper(String delimiter, int[] pointPositionInLine, Pair<float[], float[]> rangeInterpolators,
			boolean hasHeader) {
		this.delimiter = delimiter;
		this.pointPositionInLine = pointPositionInLine;
		this.rangeInterpolators = rangeInterpolators;
		this.hasHeader = hasHeader;
	}

	private Iterator<Rectangle> selectIterator(List<Rectangle> result, float[] xRange, float[] yRange, float minX,
			float maxX, float minY, float maxY) {
		if (xRange != null && yRange != null) {
			List<Rectangle> interpolatedResult = new ArrayList<Rectangle>();
			float[] initialXRange = new float[] { minX, maxX };
			float[] initialYRange = new float[] { minY, maxY };
			for (Rectangle r : result) {
				float[] limitDimensions = Rectangle.getMinMaxDimensions(r);
				float newMinX = Benchmark.interpolatePoint(limitDimensions[0], initialXRange, xRange);
				float newMaxX = Benchmark.interpolatePoint(limitDimensions[1], initialXRange, xRange);
				float newMinY = Benchmark.interpolatePoint(limitDimensions[2], initialYRange, yRange);
				float newMaxY = Benchmark.interpolatePoint(limitDimensions[3], initialYRange, yRange);
				interpolatedResult.add(new Rectangle(newMinX, newMaxX, newMinY, newMaxY));
			}
			return interpolatedResult.iterator();
		}
		return result.iterator();
	}

	@Override
	public Iterator<Rectangle> call(Iterator<String> dataIterator) throws Exception {
		List<Rectangle> result = new ArrayList<Rectangle>();
		// getting expected points position in a row
		int posMinX = pointPositionInLine[0], posMaxX = pointPositionInLine[1], posMinY = pointPositionInLine[2],
				posMaxY = pointPositionInLine[3];
		// getting range interpolators for Points
		float[] xRange = rangeInterpolators.getFirst(), yRange = rangeInterpolators.getSecond();
		// reading through file, recording min and max coordinate range
		float i = 0, minRangeX = Float.MAX_VALUE, maxRangeX = -Float.MAX_VALUE, minRangeY = Float.MAX_VALUE,
				maxRangeY = -Float.MAX_VALUE;
		while (dataIterator.hasNext()) {
			String line = dataIterator.next();
			// skipping header
			if (i == 0 && hasHeader && dataIterator.hasNext())
				line = dataIterator.next();
			String splittedRow[] = line.split(delimiter);
			// the splitted row has at least two entries
			assert splittedRow.length >= 2;
			float minX = Float.parseFloat(splittedRow[posMinX]), maxX = Float.parseFloat(splittedRow[posMaxX]),
					minY = Float.parseFloat(splittedRow[posMinY]), maxY = Float.parseFloat(splittedRow[posMaxY]);
			// updating coordinates limits
			minRangeX = Math.min(minRangeX, minX);
			maxRangeX = Math.max(maxRangeX, maxX);
			minRangeY = Math.min(minRangeY, minY);
			maxRangeY = Math.max(maxRangeY, maxY);
			result.add(new Rectangle(minX, maxX, minY, maxY));
			++i;
		}
		// interpolating results if needed
		return this.selectIterator(result, xRange, yRange, minRangeX, maxRangeX, minRangeY, maxRangeY);
	}

}
