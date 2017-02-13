/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *  
 *************************************************************************
 */

package org.orienteer.birt.orientdb.driver.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.SortSpec;
import org.eclipse.datatools.connectivity.oda.spec.QuerySpecification;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

/**
 * Implementation class of IQuery for an ODA runtime driver.
 * <br>
 * For demo purpose, the auto-generated method stubs have
 * hard-coded implementation that returns a pre-defined set
 * of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place. 
 */
public class Query implements IQuery
{
	private int m_maxRows=1;
	//private String m_preparedText;
	private ODatabaseDocument db;
	private OCommandSQL innerQuery;
	
	private Map<String,String> properties;
	private Map<String,Object> parameters;
	
	
    public Query(ODatabaseDocument db) {
    	this.db = db;
    	properties = new HashMap<String,String>();
    	parameters = new HashMap<String,Object>();
	}
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#prepare(java.lang.String)
	 */
	public void prepare( String queryText ) throws OdaException
	{
		innerQuery = new OCommandSQL(queryText);
		
        //m_preparedText = queryText;
	}
	
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setAppContext(java.lang.Object)
	 */
	public void setAppContext( Object context ) throws OdaException
	{
	    // do nothing; assumes no support for pass-through context
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#close()
	 */
	public void close() throws OdaException
	{
		innerQuery = null;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getMetaData()
	 */
	public IResultSetMetaData getMetaData() throws OdaException
	{
        /* TODO Auto-generated method stub
         * Replace with implementation to return an instance 
         * based on this prepared query.
         */
		int oldLimit = innerQuery.getLimit(); 
		innerQuery.setLimit(1);
		List<ODocument> dbResult = db.command(innerQuery).execute(parameters);
		//ODocument dbResult = (List)(db.command(innerQuery).execute(parameters));
		ResultSetMetaData result = new ResultSetMetaData(dbResult.get(0));
		innerQuery.setLimit(oldLimit);
		return result;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#executeQuery()
	 */
	public IResultSet executeQuery() throws OdaException
	{
        /* TODO Auto-generated method stub
         * Replace with implementation to return an instance 
         * based on this prepared query.
         */
		if (getMaxRows()>0){
			innerQuery.setLimit(getMaxRows());
		}
		try {
			IResultSet resultSet = new ResultSet(db.command(innerQuery).execute(parameters),(ResultSetMetaData)getMetaData());
			return resultSet;
		} catch (OCommandSQLParsingException e) {
			throw new OdaException(e);
		}
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setProperty(java.lang.String, java.lang.String)
	 */
	public void setProperty( String name, String value ) throws OdaException
	{
		// do nothing; assumes no data set query property
		properties.put(name, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setMaxRows(int)
	 */
	public void setMaxRows( int max ) throws OdaException
	{
	    m_maxRows = max;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getMaxRows()
	 */
	public int getMaxRows() throws OdaException
	{
		return m_maxRows;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#clearInParameters()
	 */
	public void clearInParameters() throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
		parameters.clear();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setInt(java.lang.String, int)
	 */
	public void setInt( String parameterName, int value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		parameters.put(parameterName, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setInt(int, int)
	 */
	public void setInt( int parameterId, int value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDouble(java.lang.String, double)
	 */
	public void setDouble( String parameterName, double value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		parameters.put(parameterName, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDouble(int, double)
	 */
	public void setDouble( int parameterId, double value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	public void setBigDecimal( String parameterName, BigDecimal value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		parameters.put(parameterName, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal( int parameterId, BigDecimal value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(java.lang.String, java.lang.String)
	 */
	public void setString( String parameterName, String value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		parameters.put(parameterName, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(int, java.lang.String)
	 */
	public void setString( int parameterId, String value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDate(java.lang.String, java.sql.Date)
	 */
	public void setDate( String parameterName, Date value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		parameters.put(parameterName, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setDate(int, java.sql.Date)
	 */
	public void setDate( int parameterId, Date value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTime(java.lang.String, java.sql.Time)
	 */
	public void setTime( String parameterName, Time value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		parameters.put(parameterName, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTime(int, java.sql.Time)
	 */
	public void setTime( int parameterId, Time value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	public void setTimestamp( String parameterName, Timestamp value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		parameters.put(parameterName, value);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp( int parameterId, Timestamp value ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to input parameter
	}

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(java.lang.String, boolean)
     */
    public void setBoolean( String parameterName, boolean value )
            throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to named input parameter
		parameters.put(parameterName, value);
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(int, boolean)
     */
    public void setBoolean( int parameterId, boolean value )
            throws OdaException
    {
        // TODO Auto-generated method stub       
        // only applies to input parameter
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setObject(java.lang.String, java.lang.Object)
     */
    public void setObject( String parameterName, Object value )
            throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to named input parameter
		parameters.put(parameterName, value);
    }
    
    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setObject(int, java.lang.Object)
     */
    public void setObject( int parameterId, Object value ) throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to input parameter
    }
    
    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(java.lang.String)
     */
    public void setNull( String parameterName ) throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to named input parameter
		parameters.put(parameterName, null);
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(int)
     */
    public void setNull( int parameterId ) throws OdaException
    {
        // TODO Auto-generated method stub
        // only applies to input parameter
    }

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#findInParameter(java.lang.String)
	 */
	public int findInParameter( String parameterName ) throws OdaException
	{
        // TODO Auto-generated method stub
		// only applies to named input parameter
		return 0;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getParameterMetaData()
	 */
	public IParameterMetaData getParameterMetaData() throws OdaException
	{
        /* TODO Auto-generated method stub
         * Replace with implementation to return an instance 
         * based on this prepared query.
         */
		return new ParameterMetaData();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#setSortSpec(org.eclipse.datatools.connectivity.oda.SortSpec)
	 */
	public void setSortSpec( SortSpec sortBy ) throws OdaException
	{
		// only applies to sorting, assumes not supported
		//sortBy.getSortOrder()
		
        throw new UnsupportedOperationException();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IQuery#getSortSpec()
	 */
	public SortSpec getSortSpec() throws OdaException
	{
		// only applies to sorting
		return null;
	}

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#setSpecification(org.eclipse.datatools.connectivity.oda.spec.QuerySpecification)
     */
    public void setSpecification( QuerySpecification querySpec )
            throws OdaException, UnsupportedOperationException
    {
        // assumes no support
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#getSpecification()
     */
    public QuerySpecification getSpecification()
    {
        // assumes no support
        return null;
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#getEffectiveQueryText()
     */
    public String getEffectiveQueryText()
    {
        // TODO Auto-generated method stub
        return innerQuery.getText();
    }

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IQuery#cancel()
     */
    public void cancel() throws OdaException, UnsupportedOperationException
    {
        // assumes unable to cancel while executing a query
        throw new UnsupportedOperationException();
    }
    
}
