package com.studql.shape;

import java.awt.Shape;

public interface Boundable {
	public Rectangle getMbr();

	public String toString();

	public boolean equals(Object obj);

	public Shape draw(float dim1, float dim2, float dim3, float dim4);
}
