package src.main.java.com.studql.utils;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import src.main.java.com.studql.rtree.Node;
import src.main.java.com.studql.rtree.Rtree;
import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;

public class Visualizer<T extends Boundable> {
	private final int image_size;

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
		// get limit values
		float x = nodeMbr.getTopLeft().getX(), y = nodeMbr.getBottomLeft().getY();
		float width = nodeMbr.getBottomRight().getX() - x, height = nodeMbr.getTopRight().getY() - y;
		// get reference range for point interpolation
		float[] widthReferenceRange = null, heightReferenceRange = null;
		float mbrWidth = rootMbrWidthRange[1] - rootMbrWidthRange[0];
		float mbrHeight = rootMbrHeightRange[1] - rootMbrHeightRange[0];
		// taking max of (width, height) root mbr as image size
		float halfImage = this.image_size / 2;
		if (mbrWidth >= mbrHeight) {
			float ratio = mbrHeight / mbrWidth;
			widthReferenceRange = new float[] { 0, this.image_size };
			heightReferenceRange = new float[] { (1 - ratio) * halfImage, (1 + ratio) * halfImage };
		} else {
			float ratio = mbrWidth / mbrHeight;
			heightReferenceRange = new float[] { 0, this.image_size };
			widthReferenceRange = new float[] { (1 - ratio) * halfImage, (1 + ratio) * halfImage };
		}
		// create drawing bounds
		int boundedX = Math.round(this.interpolatePoint(x, rootMbrWidthRange, widthReferenceRange));
		int boundedY = Math.round(this.interpolatePoint(y, rootMbrHeightRange, heightReferenceRange));
		int boundedWidth = Math.round(this.interpolateLine(width, rootMbrWidthRange, widthReferenceRange));
		int boundedHeight = Math.round(this.interpolateLine(height, rootMbrHeightRange, heightReferenceRange));
		return new int[] { boundedX, boundedY, boundedWidth, boundedHeight };
	}

	public void drawNode(Node<T> node, Graphics2D g, float[] rootMbrWidthRange, float[] rootMbrHeightRange) {
		if (node != null) {
			Rectangle mbr;
			// if node is record
			if (node.getRecord() != null) {
				g.setPaint(Color.RED);
				mbr = node.getRecord().getMbr();
			} else {
				g.setPaint(Color.BLUE);
				mbr = node.getMbr();
			}
			int[] drawingDimensions = this.getDrawingDimensions(mbr, rootMbrWidthRange, rootMbrHeightRange);
			g.drawRect(drawingDimensions[0], drawingDimensions[1], drawingDimensions[2], drawingDimensions[3]);

			List<Node<T>> children = node.getChildren();
			if (children != null) {
				for (Node<T> child : children)
					this.drawNode(child, g, rootMbrWidthRange, rootMbrHeightRange);
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
		float[] rootMbrWidthRange = new float[] { rootMbr.getTopLeft().getX(), rootMbr.getTopRight().getX() };
		float[] rootMbrHeightRange = new float[] { rootMbr.getBottomLeft().getY(), rootMbr.getTopLeft().getY() };
		// draw tree recursively
		this.drawNode(tree.getRoot(), graphics2D, rootMbrWidthRange, rootMbrHeightRange);
		graphics2D.dispose();

		ImageIO.write(image, "png", filelocation);
	}
}
