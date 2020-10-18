package src.main.java.com.studql.shape;

public final class Rectangle {
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

	public boolean contains(Rectangle r) {
		float minInstanceX = this.topLeft.getX(), maxInstanceY = this.topLeft.getY();
		float maxInstanceX = this.bottomRight.getX(), minInstanceY = this.bottomRight.getY();
		float minRectX = r.getTopLeft().getX(), maxRectY = r.getTopLeft().getY();
		float maxRectX = r.getBottomRight().getX(), minRectY = r.getBottomRight().getY();
		return minInstanceX <= minRectX && maxInstanceX >= maxRectX && minInstanceY <= minRectY
				&& maxInstanceY >= maxRectY;
	}

	public float area() {
		float length = this.topRight.getX() - this.topLeft.getX();
		float height = this.topLeft.getY() - this.bottomLeft.getY();
		return length * height;
	}

}
