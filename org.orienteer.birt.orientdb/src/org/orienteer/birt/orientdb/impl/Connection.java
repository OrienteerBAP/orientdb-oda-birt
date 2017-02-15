/*
 *************************************************************************
 * Copyright (c) 2014 <<Your Company Name here>>
 *  
 *************************************************************************
 */

package org.orienteer.birt.orientdb.impl;

import java.util.Properties;
import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDataSetMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.ibm.icu.util.ULocale;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * Implementation class of IConnection for an ODA runtime driver.
 */
public class Connection implements IConnection
{
    private boolean m_isOpen = false;
    private ODatabaseDocumentTx db;
    private static final String DB_URI_PROPERTY="uri";
    private static final String DB_USER_PROPERTY="user";
    private static final String DB_PASSWORD_PROPERTY="password";
    
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#open(java.util.Properties)
	 */
	@SuppressWarnings("resource")
	public void open( Properties connProperties ) throws OdaException
	{
		String url = connProperties.getProperty(DB_URI_PROPERTY);//"remote:127.0.0.1/Orienteer";
		String username = connProperties.getProperty(DB_USER_PROPERTY);//"admin";
		String password = connProperties.getProperty(DB_PASSWORD_PROPERTY);//"admin";
		
		try {
			db = new ODatabaseDocumentTx(url).open(username, password);
		    m_isOpen = true;        
		} catch (Exception e) {
			throw new OdaException(e);
		}
 	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#setAppContext(java.lang.Object)
	 */
	public void setAppContext( Object context ) throws OdaException
	{
	    // do nothing; assumes no support for pass-through context
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#close()
	 */
	public void close() throws OdaException
	{
		if (db!=null && !db.isClosed()){
			db.close();
		}
		db = null;
	    m_isOpen = false;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#isOpen()
	 */
	public boolean isOpen() throws OdaException
	{
		return m_isOpen;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#getMetaData(java.lang.String)
	 */
	public IDataSetMetaData getMetaData( String dataSetType ) throws OdaException
	{
	    // assumes that this driver supports only one type of data set,
        // ignores the specified dataSetType
		return new DataSetMetaData( this );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#newQuery(java.lang.String)
	 */
	public IQuery newQuery( String dataSetType ) throws OdaException
	{
		//dbConnect.
		
        // assumes that this driver supports only one type of data set,
        // ignores the specified dataSetType
		return new Query(db);
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#getMaxQueries()
	 */
	public int getMaxQueries() throws OdaException
	{
		return 0;	// no limit
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#commit()
	 */
	public void commit() throws OdaException
	{
		db.commit();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IConnection#rollback()
	 */
	public void rollback() throws OdaException
	{
		db.rollback();
	}

    /* (non-Javadoc)
     * @see org.eclipse.datatools.connectivity.oda.IConnection#setLocale(com.ibm.icu.util.ULocale)
     */
    public void setLocale( ULocale locale ) throws OdaException
    {
        // do nothing; assumes no locale support
    }
    
}
