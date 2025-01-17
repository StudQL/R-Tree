package com.studql.shape;

import java.awt.Shape;
import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.studql.utils.Pair;

public final class Rectangle implements Boundable, Serializable, Comparable<Rectangle> {

	private static final long serialVersionUID = 685100720547490483L;
	private final Point topLeft;
	private final Point bottomRight;
	private final Point topRight;
	private final Point bottomLeft;

	public Rectangle(Point topLeft, Point bottomRight) {
		this.topLeft = topLeft;
		this.bottomRight = bottomRight;
		this.topRight = new Point(bottomRight.getX(), topLeft.getY());
		this.bottomLeft = new Point(topLeft.getX(), bottomRight.getY());
	}

	public Rectangle(Point unitPoint) {
		this.topLeft = unitPoint;
		this.topRight = unitPoint;
		this.bottomLeft = unitPoint;
		this.bottomRight = unitPoint;
	}

	public Rectangle(float minX, float maxX, float minY, float maxY) {
		this.topLeft = new Point(minX, maxY);
		this.bottomRight = new Point(maxX, minY);
		this.topRight = new Point(maxX, maxY);
		this.bottomLeft = new Point(minX, minY);
	}

	public Point getTopLeft() {
		return this.topLeft;
	}

	public Point getBottomRight() {
		return this.bottomRight;
	}

	public Point getTopRight() {
		return this.topRight;
	}

	public Point getBottomLeft() {
		return this.bottomLeft;
	}

	public Rectangle getMbr() {
		return this;
	}

	public float lowest() {
		return topRight.SumCoord();
	}

	public float highest() {
		return bottomLeft.SumCoord();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		Rectangle r = (Rectangle) obj;
		return r.topLeft.equals(this.topLeft) && r.topRight.equals(this.topRight)
				&& r.bottomLeft.equals(this.bottomLeft) && r.bottomRight.equals(this.bottomRight);
	}

	public static float[] getMinMaxDimensions(Rectangle r) {
		float minX = r.topLeft.getX(), maxY = r.topLeft.getY();
		float maxX = r.bottomRight.getX(), minY = r.bottomRight.getY();
		return new float[] { minX, maxX, minY, maxY };
	}

	public static Rectangle buildRectangle(Rectangle r1, Rectangle r2) {
		float[] r1Limits = getMinMaxDimensions(r1);
		float[] r2Limits = getMinMaxDimensions(r2);
		float minX = Math.min(r1Limits[0], r2Limits[0]);
		float maxX = Math.max(r1Limits[1], r2Limits[1]);
		float minY = Math.min(r1Limits[2], r2Limits[2]);
		float maxY = Math.max(r1Limits[3], r2Limits[3]);
		return new Rectangle(minX, maxX, minY, maxY);
	}

	public boolean contains(Rectangle r) {
		float[] minInstanceDimensions = getMinMaxDimensions(this);
		float[] minRectDimensions = getMinMaxDimensions(r);
		return minInstanceDimensions[0] <= minRectDimensions[0] && minInstanceDimensions[1] >= minRectDimensions[1]
				&& minInstanceDimensions[2] <= minRectDimensions[2] && minInstanceDimensions[3] >= minRectDimensions[3];
	}

	public boolean isOverLapping(Rectangle r) {
		if (this.topLeft.getX() > r.bottomRight.getX() // this is right to r
				|| this.bottomRight.getX() < r.topLeft.getX() // this is left to r
				|| this.topLeft.getY() < r.bottomRight.getY() // this is above r
				|| this.bottomRight.getY() > r.topLeft.getY()) { // this is below r
			return false;
		}
		return true;
	}

	public float area() {
		float length = this.topRight.getX() - this.topLeft.getX();
		float height = this.topLeft.getY() - this.bottomLeft.getY();
		return length * height;
	}

	private Point getCenter() {
		float centerX = ((this.getBottomRight().getX() - this.getBottomLeft().getX()) / 2)
				+ this.getBottomLeft().getX();
		float centerY = ((this.getTopRight().getY() - this.getBottomRight().getY()) / 2) + this.getBottomLeft().getY();
		return new Point(centerX, centerY);
	}

	private Pair<Point, Point> getClosestCorners(Rectangle r) {
		float distTopBottom = Math.abs(this.getTopLeft().getY() - r.getBottomLeft().getY());
		float distBottomTop = Math.abs(this.getBottomLeft().getY() - r.getTopLeft().getY());
		List<Point> cornersInstance = new ArrayList<Point>();
		List<Point> cornersR = new ArrayList<Point>();
		if (distBottomTop < distTopBottom) {
			cornersInstance.add(this.getBottomLeft());
			cornersInstance.add(this.getBottomRight());
			cornersR.add(r.getTopLeft());
			cornersR.add(r.getTopRight());
		} else {
			cornersInstance.add(this.getTopLeft());
			cornersInstance.add(this.getTopRight());
			cornersR.add(r.getBottomLeft());
			cornersR.add(r.getBottomRight());
		}
		return Point.getClosestPair(cornersInstance, cornersR);
	}

	// closest corner distance
	public float distance(Rectangle r) {
		if (this.isOverLapping(r))
			return 0;
		Pair<Point, Point> closestCorners = this.getClosestCorners(r);
		Point p1 = closestCorners.getFirst(), p2 = closestCorners.getSecond();
		return p1.euclidianDistance(p2);
	}

	public float intersectArea(Rectangle r) {
		float farthestLeft = Math.max(r.getTopLeft().getX(), this.getTopLeft().getX());
		float nearestRight = Math.min(r.getTopRight().getX(), this.getTopRight().getX());
		float length = nearestRight - farthestLeft;
		if (length <= 0)
			return 0;
		float farthestBottom = Math.max(r.getBottomLeft().getY(), this.getBottomLeft().getY());
		float nearestTop = Math.min(r.getTopLeft().getY(), this.getTopLeft().getY());
		float height = nearestTop - farthestBottom;
		if (height <= 0)
			return 0;
		return length * height;
	}

	public float calculateEnlargement(Rectangle r) {
		Rectangle overlappingRectangle = Rectangle.buildRectangle(this, r);
		return overlappingRectangle.area() - this.area();
	}

	public String toString() {
		return "[" + this.bottomLeft.getX() + ", " + this.bottomRight.getX() + ", " + this.bottomLeft.getY() + ", "
				+ this.topLeft.getY() + "]";
	}

	public Shape draw(float dim1, float dim2, float dim3, float dim4) {
		return new Rectangle2D.Float(dim1, dim2, dim3, dim4);
	}

	public int compareTo(Rectangle r) {
		Point instanceCenter = getCenter();
		Point rCenter = r.getCenter();
		double instanceDistanceToOrigin = Math
				.sqrt(instanceCenter.getX() * instanceCenter.getX() + instanceCenter.getY() * instanceCenter.getY());
		double rDistanceToOrigin = Math.sqrt(rCenter.getX() * rCenter.getX() + rCenter.getY() * rCenter.getY());
		return Double.compare(instanceDistanceToOrigin, rDistanceToOrigin);
	}

}
