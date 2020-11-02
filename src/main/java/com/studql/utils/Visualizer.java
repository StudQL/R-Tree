package src.main.java.com.studql.utils;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import javax.imageio.ImageIO;

import src.main.java.com.studql.rtree.Rtree;
import src.main.java.com.studql.rtree.node.Node;
import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Point;
import src.main.java.com.studql.shape.Rectangle;

public class Visualizer<T extends Boundable> {
	private final int image_size;
	private static final int PADDING = 40;
	private static final int OVERLAP_TRANSPARENCY = 20;
	// bright pink
	private static final Color RECORD_COLOR = new Color(254, 2, 164);

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
		// get limit values for node
		float x = nodeMbr.getTopLeft().getX(), y = nodeMbr.getTopLeft().getY();
		float width = nodeMbr.getBottomRight().getX() - x, height = y - nodeMbr.getBottomLeft().getY();
		// taking max of (width, height) root mbr as image size
		float borderRatio = 0;
		float[] floatYRange;
		// this section computer interpolation points as a function of the max between w
		// and h of image
		if (mbrWidth >= mbrHeight) {
			borderRatio = (mbrWidth - mbrHeight) / 2;
			// rescaling y because drawing on canvas is inverted (0, 0) represents top left
			y = borderRatio + (rootMbrHeightRange[1] - y);
			floatYRange = new float[] { borderRatio, borderRatio + mbrHeight };
			// adding adding so that image doesn't touch borders
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

	private void drawRecordNode(Graphics2D g, Node<T> node, float[] rootMbrWidthRange, float[] rootMbrHeightRange) {
		g.setPaint(RECORD_COLOR);
		T recordValue = node.getRecord().getValue();
		Rectangle mbr = recordValue.getMbr();
		int[] drawingDimensions = this.getDrawingDimensions(mbr, rootMbrWidthRange, rootMbrHeightRange);
		// drawing a small square for point
		if (recordValue instanceof Point)
			g.fill(recordValue.draw(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2],
					drawingDimensions[3]));
		else
			g.draw(recordValue.draw(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2],
					drawingDimensions[3]));
	}

	private void drawInternalNode(Graphics2D g, Node<T> node, float[] rootMbrWidthRange, float[] rootMbrHeightRange,
			int nodeHeight, int treeHeight) {
		// level of green
		float interpolatedWithHeight = this.interpolatePoint(nodeHeight, new float[] { treeHeight, 0 },
				new float[] { 0, 255 });
		int interpolatedGreenValue = Math.round(interpolatedWithHeight);
		// create colors proportional to depth of node
		Color borderColor = new Color(0, interpolatedGreenValue, 255);
		Color fillColor = new Color(0, interpolatedGreenValue, 255, OVERLAP_TRANSPARENCY);
		g.setPaint(borderColor);
		Rectangle mbr = node.getMbr();
		int[] drawingDimensions = this.getDrawingDimensions(mbr, rootMbrWidthRange, rootMbrHeightRange);
		// draw border
		g.draw(mbr.draw(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2], drawingDimensions[3]));
		g.setPaint(fillColor);
		// fill with almost transparent to visualize overlap
		g.fill(mbr.draw(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2], drawingDimensions[3]));
	}

	public void drawNodes(Node<T> root, Graphics2D g, float[] rootMbrWidthRange, float[] rootMbrHeightRange,
			int treeHeight) {
		// performing level order traversal to have the records drawn last all at once
		Queue<Node<T>> queue = new LinkedList<Node<T>>();
		queue.add(root);
		queue.add(null);
		int nodeHeight = 0;
		while (!queue.isEmpty()) {
			Node<T> current = queue.poll();
			// draw current node
			if (current != null) {
				if (current.getRecord() != null)
					this.drawRecordNode(g, current, rootMbrWidthRange, rootMbrHeightRange);
				else
					this.drawInternalNode(g, current, rootMbrWidthRange, rootMbrHeightRange, nodeHeight, treeHeight);
				// add next level
				List<Node<T>> children = current.getChildren();
				if (children != null) {
					for (Node<T> child : children)
						queue.add(child);
				}
			}
			// reached next level
			else if (!queue.isEmpty()) {
				++nodeHeight;
				queue.add(null);
			}
		}

	}

	public void createVisualization(Rtree<T> tree, File filelocation) throws IOException {
		final BufferedImage image = new BufferedImage(this.image_size, this.image_size, BufferedImage.TYPE_INT_ARGB);
		final Graphics2D graphics2D = image.createGraphics();
		graphics2D.setPaint(Color.WHITE);
		graphics2D.fillRect(0, 0, this.image_size, this.image_size);
		// getting root's mbr limit dimensions
		Rectangle rootMbr = tree.getRoot().getMbr();
		int treeHeight = tree.calculateHeight();
		float[] rootMbrWidthRange = new float[] { rootMbr.getTopLeft().getX(), rootMbr.getTopRight().getX() };
		float[] rootMbrHeightRange = new float[] { rootMbr.getBottomLeft().getY(), rootMbr.getTopLeft().getY() };
		// draw tree recursively
		this.drawNodes(tree.getRoot(), graphics2D, rootMbrWidthRange, rootMbrHeightRange, treeHeight);
		graphics2D.dispose();

		ImageIO.write(image, "png", filelocation);
	}
}
