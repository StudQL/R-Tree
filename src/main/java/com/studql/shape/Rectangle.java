package src.main.java.com.studql.shape;

import java.util.ArrayList;

public final class Rectangle implements Boundable {
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

	@SuppressWarnings("serial")
	public static ArrayList<Float> getMinMaxDimensions(Rectangle r) {
		float minX = r.topLeft.getX(), maxY = r.topLeft.getY();
		float maxX = r.bottomRight.getX(), minY = r.bottomRight.getY();
		return new ArrayList<Float>() {
			{
				add(minX);
				add(maxX);
				add(minY);
				add(maxY);
			}
		};
	}

	public static Rectangle buildRectangle(Rectangle r1, Rectangle r2) {
		ArrayList<Float> r1Limits = getMinMaxDimensions(r1);
		ArrayList<Float> r2Limits = getMinMaxDimensions(r2);
		float minX = Math.min(r1Limits.get(0), r2Limits.get(0));
		float maxX = Math.max(r1Limits.get(1), r2Limits.get(1));
		float minY = Math.min(r1Limits.get(2), r2Limits.get(2));
		float maxY = Math.max(r1Limits.get(3), r2Limits.get(3));
		return new Rectangle(minX, maxX, minY, maxY);
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
		return r.topLeft == this.topLeft && r.topRight == this.topRight && r.bottomLeft == this.bottomLeft
				&& r.bottomRight == this.bottomRight;
	}

	public Rectangle getMbr() {
		return this;
	}

	public boolean contains(Rectangle r) {
		ArrayList<Float> minInstanceDimensions = getMinMaxDimensions(this);
		ArrayList<Float> minRectDimensions = getMinMaxDimensions(r);
		return minInstanceDimensions.get(0) <= minRectDimensions.get(0)
				&& minInstanceDimensions.get(1) >= minRectDimensions.get(1)
				&& minInstanceDimensions.get(2) <= minRectDimensions.get(2)
				&& minInstanceDimensions.get(3) >= minRectDimensions.get(3);
	}

	public float area() {
		float length = this.topRight.getX() - this.topLeft.getX();
		float height = this.topLeft.getY() - this.bottomLeft.getY();
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

}
