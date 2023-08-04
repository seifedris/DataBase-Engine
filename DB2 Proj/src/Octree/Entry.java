package Octree;

public class Entry implements java.io.Serializable{
	private Object value;
	private String pageId;

	public Entry(Object value, String pageId) {

		this.value = value;
		this.pageId = pageId;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

}
