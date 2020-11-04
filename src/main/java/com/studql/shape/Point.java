package com.studql.shape;

import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.io.Serializable;
import java.util.List;

import com.studql.utils.Pair;

public final class Point implements Boundable, Serializable, Comparable<Point> {

	private static final long serialVersionUID = 6795023477825593653L;
	private static final int DRAW_SIZE = 3;
	private final float x;
	private final float y;
	// mbr for point is just a unit rectangle
	private final Rectangle mbr;

	public Point(float x, float y) {
		this.x = x;
		this.y = y;
		this.mbr = new Rectangle(this);
	}

	public float getX() {
		return x;
	}

	public float getY() {
		return y;
	}

	public Rectangle getMbr() {
		return this.mbr;
	}

	public float SumCoord() {
		return x + y;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		Point p = (Point) obj;
		return p.x == this.x && p.y == this.y;
	}

	public String toString() {
		return "[" + this.x + ", " + this.y + "]";
	}

	public float euclidianDistance(Point p) {
		float xDiff = Math.abs(this.getX() - p.getX());
		float yDiff = Math.abs(this.getY() - p.getY());
		return (float) Math.sqrt(Math.pow(xDiff, 2) + Math.pow(yDiff, 2));
	}

	public static Pair<Point, Point> getClosestPair(List<Point> firstPoints, List<Point> secondPoints) {
		Pair<Point, Point> result = null;
		float minDistance = Float.MAX_VALUE;
		for (Point pFirst : firstPoints) {
			for (Point pSecond : secondPoints) {
				float currentDistance = pFirst.euclidianDistance(pSecond);
				if (currentDistance < minDistance) {
					minDistance = currentDistance;
					result = new Pair<Point, Point>(pFirst, pSecond);
				}
			}
		}
		return result;
	}

	public Shape draw(float dim1, float dim2, float dim3, float dim4) {
		return new Ellipse2D.Float(dim1 - DRAW_SIZE, dim2 - DRAW_SIZE, DRAW_SIZE * 2, DRAW_SIZE * 2);
	}

	public int compareTo(Point p) {
		double instanceDistanceToOrigin = Math.sqrt(x * x + y * y);
		double pDistanceToOrigin = Math.sqrt(p.getX() * p.getX() + p.getY() * p.getY());
		return Double.compare(instanceDistanceToOrigin, pDistanceToOrigin);
	}

}
