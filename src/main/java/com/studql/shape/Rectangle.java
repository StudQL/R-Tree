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

	public Rectangle(float xTopLeft, float yTopLeft, float xBottomRight, float yBottomRight) {
		this.topLeft = new Point(xTopLeft, yTopLeft);
		this.bottomRight = new Point(xBottomRight, yBottomRight);
		this.topRight = new Point(xBottomRight, yTopLeft);
		this.bottomLeft = new Point(xTopLeft, yBottomRight);
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
		float minInstanceX = r.topLeft.getX(), maxInstanceY = r.topLeft.getY();
		float maxInstanceX = r.bottomRight.getX(), minInstanceY = r.bottomRight.getY();
		return new ArrayList<Float>() {
			{
				add(minInstanceX);
				add(maxInstanceX);
				add(minInstanceY);
				add(maxInstanceY);
			}
		};
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
		float enlargement = 0;
		ArrayList<Float> minInstanceDimensions = getMinMaxDimensions(this);
		ArrayList<Float> minRectDimensions = getMinMaxDimensions(r);
		enlargement += Math.max(minInstanceDimensions.get(0) - minRectDimensions.get(0), 0);
		enlargement += Math.max(minRectDimensions.get(1) - minInstanceDimensions.get(1), 0);
		enlargement += Math.max(minInstanceDimensions.get(2) - minRectDimensions.get(2), 0);
		enlargement += Math.max(minRectDimensions.get(3) - minInstanceDimensions.get(3), 0);
		return enlargement;
	}

}
