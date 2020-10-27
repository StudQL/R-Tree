package src.main.java.com.studql.shape;

import java.awt.Shape;
import java.awt.geom.Ellipse2D;

public final class Point implements Boundable {
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

	public Shape draw(float dim1, float dim2, float dim3, float dim4) {
		return new Ellipse2D.Float(dim1 - DRAW_SIZE, dim2 - DRAW_SIZE, DRAW_SIZE * 2, DRAW_SIZE * 2);
	}

}
