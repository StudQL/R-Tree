package src.main.java.com.studql.shape;

public final class Point implements Boundable {
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

	public Rectangle getMbr() {
		return this.mbr;
	}

	public String toString() {
		return "[" + this.x + ", " + this.y + "]";
	}

}
