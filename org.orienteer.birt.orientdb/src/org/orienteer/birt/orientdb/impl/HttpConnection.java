package org.orienteer.birt.orientdb.impl;

import java.util.Properties;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDataSetMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.ibm.icu.util.ULocale;


public class HttpConnection implements IConnection{

	private boolean isOpen = false;
	private Properties properties;
	
	
	@Override
	public void open(Properties arg0) throws OdaException {
		isOpen = true;
		properties = arg0;
	}

	@Override
	public IQuery newQuery(String arg0) throws OdaException {
		return new HttpQuery(this);
	}

	@Override
	public void close() throws OdaException {
		isOpen = false;
	}
	
	public Properties getProperties() {
		return properties;
	}
	
	@Override
	public void commit() throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getMaxQueries() throws OdaException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public IDataSetMetaData getMetaData(String arg0) throws OdaException {
		return new DataSetMetaData( this );
	}

	@Override
	public boolean isOpen() throws OdaException {
		return isOpen;
	}

	@Override
	public void rollback() throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAppContext(Object arg0) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLocale(ULocale arg0) throws OdaException {
		// TODO Auto-generated method stub
		
	}
}
