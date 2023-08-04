package SQLTerm;

public class SQLTerm {
	public String _strTableName;
	public String _strColumnName;
	public String _strOperator;
	public Object _objValue;
	public SQLTerm() {
		_strTableName=new String();
		_strColumnName=new String();
		_strOperator=new String();
		_objValue=new Object();
	}
}
