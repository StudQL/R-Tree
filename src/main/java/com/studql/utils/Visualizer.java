package src.main.java.com.studql.utils;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import src.main.java.com.studql.rtree.Rtree;
import src.main.java.com.studql.rtree.node.Node;
import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Point;
import src.main.java.com.studql.shape.Rectangle;

public class Visualizer<T extends Boundable> {
	private final int image_size;
	private static final int PADDING = 40;

	public Visualizer() {
		this.image_size = 1000;
	}

	public Visualizer(int image_size) {
		this.image_size = image_size;
	}

	private float interpolatePoint(float value, float[] valueRange, float[] referenceRange) {
		return referenceRange[0]
				+ (value - valueRange[0]) * ((referenceRange[1] - referenceRange[0]) / (valueRange[1] - valueRange[0]));
	}

	private float interpolateLine(float value, float[] valueRange, float[] referenceRange) {
		return (value / (valueRange[1] - valueRange[0])) * (referenceRange[1] - referenceRange[0]);
	}

	private int[] getDrawingDimensions(Rectangle nodeMbr, float[] rootMbrWidthRange, float[] rootMbrHeightRange) {
		// get reference range for point interpolation
		float[] widthReferenceRange = null, heightReferenceRange = null;
		float mbrWidth = rootMbrWidthRange[1] - rootMbrWidthRange[0];
		float mbrHeight = rootMbrHeightRange[1] - rootMbrHeightRange[0];
		// get limit values
		float x = nodeMbr.getTopLeft().getX(), y = nodeMbr.getTopLeft().getY();
		float width = nodeMbr.getBottomRight().getX() - x, height = y - nodeMbr.getBottomLeft().getY();
		// taking max of (width, height) root mbr as image size
		float borderRatio = 0;
		float[] floatYRange;
		if (mbrWidth >= mbrHeight) {
			borderRatio = (mbrWidth - mbrHeight) / 2;
			y = borderRatio + (rootMbrHeightRange[1] - y);
			floatYRange = new float[] { borderRatio, borderRatio + mbrHeight };
			widthReferenceRange = new float[] { PADDING, this.image_size - PADDING };
			heightReferenceRange = new float[] { borderRatio * (this.image_size - 2 * PADDING) / mbrWidth,
					(mbrWidth - borderRatio) * (this.image_size - 2 * PADDING) / mbrWidth };
		} else {
			borderRatio = (mbrHeight - mbrWidth) / 2;
			y = mbrHeight - y + 1;
			floatYRange = new float[] { 0, mbrHeight };
			heightReferenceRange = new float[] { PADDING, this.image_size - PADDING };
			widthReferenceRange = new float[] { borderRatio * (this.image_size - 2 * PADDING) / mbrHeight,
					(mbrHeight - borderRatio) * (this.image_size - 2 * PADDING) / mbrHeight };
		}
		// create drawing bounds
		int boundedX = Math.round(this.interpolatePoint(x, rootMbrWidthRange, widthReferenceRange));
		int boundedY = Math.round(this.interpolatePoint(y, floatYRange, heightReferenceRange));
		int boundedWidth = Math.round(this.interpolateLine(width, rootMbrWidthRange, widthReferenceRange));
		int boundedHeight = Math.round(this.interpolateLine(height, rootMbrHeightRange, heightReferenceRange));
		return new int[] { boundedX, boundedY, boundedWidth, boundedHeight };
	}

	public void drawNode(Node<T> node, Graphics2D g, float[] rootMbrWidthRange, float[] rootMbrHeightRange,
			int nodeHeight, int treeHeight) {
		if (node != null) {
			List<Node<T>> children = node.getChildren();
			if (children != null) {
				for (Node<T> child : children)
					this.drawNode(child, g, rootMbrWidthRange, rootMbrHeightRange, nodeHeight - 1, treeHeight);
			}
			if (node.getRecord() != null) {
				g.setPaint(Color.RED);
				T recordValue = node.getRecord().getValue();
				Rectangle mbr = recordValue.getMbr();
				int[] drawingDimensions = this.getDrawingDimensions(mbr, rootMbrWidthRange, rootMbrHeightRange);
				if (recordValue instanceof Point)
					g.fill(recordValue.draw(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2],
							drawingDimensions[3]));
				else
					g.draw(recordValue.draw(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2],
							drawingDimensions[3]));
			} else {
				g.setPaint(new Color(0, Math.round(
						this.interpolatePoint(nodeHeight, new float[] { 0, treeHeight }, new float[] { 0, 255 })),
						255));
				Rectangle mbr = node.getMbr();
				int[] drawingDimensions = this.getDrawingDimensions(mbr, rootMbrWidthRange, rootMbrHeightRange);
				g.draw(mbr.draw(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2],
						drawingDimensions[3]));
			}
		}
	}

	public void createVisualization(Rtree<T> tree, File filelocation) throws IOException {
		final BufferedImage image = new BufferedImage(this.image_size, this.image_size, BufferedImage.TYPE_INT_ARGB);
		final Graphics2D graphics2D = image.createGraphics();
		graphics2D.setPaint(Color.WHITE);
		graphics2D.fillRect(0, 0, 1000, 1000);
		graphics2D.setPaint(Color.BLACK);
		// getting root's mbr limit dimensions
		Rectangle rootMbr = tree.getRoot().getMbr();
		int treeHeight = tree.calculateHeight();
		float[] rootMbrWidthRange = new float[] { rootMbr.getTopLeft().getX(), rootMbr.getTopRight().getX() };
		float[] rootMbrHeightRange = new float[] { rootMbr.getBottomLeft().getY(), rootMbr.getTopLeft().getY() };
		// draw tree recursively
		this.drawNode(tree.getRoot(), graphics2D, rootMbrWidthRange, rootMbrHeightRange, treeHeight, treeHeight);
		graphics2D.dispose();

		ImageIO.write(image, "png", filelocation);
	}
}
