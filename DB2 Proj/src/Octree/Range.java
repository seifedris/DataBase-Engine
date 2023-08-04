package Octree;

public class Range implements java.io.Serializable{
	private Object min1;
	private Object max1;
	private Object min2;
	private Object max2;
	private Object min3;
	private Object max3;

	public Range(Object min1, Object max1, Object min2, Object max2, Object min3, Object max3) {
		this.min1 = min1;
		this.max1 = max1;
		this.min2 = min2;
		this.max2 = max2;
		this.min3 = min3;
		this.max3 = max3;
	}

	public Object getMin1() {
		return min1;
	}

	public void setMin1(Object min1) {
		this.min1 = min1;
	}

	public Object getMax1() {
		return max1;
	}

	public void setMax1(Object max1) {
		this.max1 = max1;
	}

	public Object getMin2() {
		return min2;
	}

	public void setMin2(Object min2) {
		this.min2 = min2;
	}

	public Object getMax2() {
		return max2;
	}

	public void setMax2(Object max2) {
		this.max2 = max2;
	}

	public Object getMin3() {
		return min3;
	}

	public void setMin3(Object min3) {
		this.min3 = min3;
	}

	public Object getMax3() {
		return max3;
	}

	public void setMax3(Object max3) {
		this.max3 = max3;
	}

}
