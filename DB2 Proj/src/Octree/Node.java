package Octree;

public class Node implements java.io.Serializable{
	Range range;
	Position position;
	public Node(Range range,Position position) {
		this.range = range;
		this.position=position;
	}
	
	
	public Range getRange() {
		return range;
	}
	public void setRange(Range range) {
		this.range = range;
	}
	public Position getPosition() {
		return position;
	}
	public void setPosition(Position position) {
		this.position = position;
	}
	

}
