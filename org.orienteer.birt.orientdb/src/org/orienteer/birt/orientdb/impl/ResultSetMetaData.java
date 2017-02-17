/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *  
 *************************************************************************
 */

package org.orienteer.birt.orientdb.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * Implementation class of IResultSetMetaData for an ODA runtime driver.
 * <br>
 * For demo purpose, the auto-generated method stubs have
 * hard-coded implementation that returns a pre-defined set
 * of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place. 
 */
public class ResultSetMetaData implements IResultSetMetaData
{
	  private final static Map<OType, Integer> typesSqlTypes = new HashMap<OType, Integer>();

	  static {
	    typesSqlTypes.put(OType.STRING, Types.VARCHAR);
	    typesSqlTypes.put(OType.INTEGER, Types.INTEGER);
	    typesSqlTypes.put(OType.FLOAT, Types.FLOAT);
	    typesSqlTypes.put(OType.SHORT, Types.SMALLINT);
	    typesSqlTypes.put(OType.BOOLEAN, Types.BOOLEAN);
	    typesSqlTypes.put(OType.LONG, Types.BIGINT);
	    typesSqlTypes.put(OType.DOUBLE, Types.DOUBLE);
	    typesSqlTypes.put(OType.DATE, Types.DATE);
	    typesSqlTypes.put(OType.DATETIME, Types.TIMESTAMP);
	    typesSqlTypes.put(OType.BYTE, Types.TINYINT);
	    typesSqlTypes.put(OType.SHORT, Types.SMALLINT);
	    typesSqlTypes.put(OType.DECIMAL, Types.DECIMAL);

	    // NOT SURE ABOUT THE FOLLOWING MAPPINGS
	    typesSqlTypes.put(OType.BINARY, Types.JAVA_OBJECT);//Types.BINARY);
	    typesSqlTypes.put(OType.EMBEDDED, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.EMBEDDEDLIST, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.EMBEDDEDMAP, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.EMBEDDEDSET, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.LINK, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.LINKLIST, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.LINKMAP, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.LINKSET, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.LINKBAG, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.ANY, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.CUSTOM, Types.JAVA_OBJECT);
	    typesSqlTypes.put(OType.TRANSIENT, Types.NULL);
	}
	private final static Map<Integer, Boolean> sqlTypesMayBeNull = new HashMap<Integer, Boolean>();
	static {
		sqlTypesMayBeNull.put(Types.JAVA_OBJECT, true);
		sqlTypesMayBeNull.put(Types.NULL, true);
		sqlTypesMayBeNull.put(Types.VARCHAR, true);
		sqlTypesMayBeNull.put(Types.VARCHAR, true);
	}
	
	
	private ODocument rowDoc;
	private List<String> fieldsNames;
	
	/*
	public ResultSetMetaData() {
		this(new ODocument().field("emptyResult", ""));
	}
	*/
	public ResultSetMetaData(ODocument rowDoc) {
		this.rowDoc = rowDoc;
		fieldsNames = Arrays.asList(rowDoc.fieldNames());
	}
	
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnCount()
	 */
	public int getColumnCount() throws OdaException
	{
        return fieldsNames.size();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnName(int)
	 */
	public String getColumnName( int index ) throws OdaException
	{
        return fieldsNames.get(index-1);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnLabel(int)
	 */
	public String getColumnLabel( int index ) throws OdaException
	{
		return getColumnName( index );		// default
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnType(int)
	 */
	public int getColumnType( int index ) throws OdaException
	{
		//return Types.JAVA_OBJECT;//Any OrientDB field may be NULL
		///*
		String fieldName = getColumnName(index);
		OType fieldType = rowDoc.fieldType(fieldName);
		if (fieldType!=null){
			return typesSqlTypes.get(fieldType);
		}else{
			return Types.VARCHAR;
			//throw new OdaException("Unknown OType of field "+fieldName+" with index "+index);
		}
		//*/
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnTypeName(int)
	 */
	public String getColumnTypeName( int index ) throws OdaException
	{
        int nativeTypeCode = getColumnType( index );
        return Driver.getNativeDataTypeName( nativeTypeCode );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnDisplayLength(int)
	 */
	public int getColumnDisplayLength( int index ) throws OdaException
	{
        // TODO replace with data source specific implementation

        // hard-coded for demo purpose
		return 8;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getPrecision(int)
	 */
	public int getPrecision( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
		return -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getScale(int)
	 */
	public int getScale( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
		return -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#isNullable(int)
	 */
	public int isNullable( int index ) throws OdaException
	{
		int type =  getColumnType( index );
		if (sqlTypesMayBeNull.containsKey(type)){
			return IResultSetMetaData.columnNullable;
		}else{
			return IResultSetMetaData.columnNoNulls;
		}
	}
    
    public int getColumnId( String columnName ) throws OdaException
    {
    	return fieldsNames.indexOf(columnName)+1;
    }
}
