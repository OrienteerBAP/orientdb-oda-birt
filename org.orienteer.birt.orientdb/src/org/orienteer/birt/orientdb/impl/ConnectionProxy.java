package org.orienteer.birt.orientdb.impl;

import java.util.Properties;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDataSetMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.OdaException;

import com.ibm.icu.util.ULocale;

public class ConnectionProxy implements IConnection{
	
	private IConnection innerConnection;
	
	
	@Override
	public void open(Properties arg0) throws OdaException {
		innerConnection = makeInnerConnection(arg0);
		innerConnection.open(arg0);
	}
	
	private IConnection makeInnerConnection(Properties connProperties) {
		String url = connProperties.getProperty(Connection.DB_URI_PROPERTY);//"remote:127.0.0.1/Orienteer";
		//String username = connProperties.getProperty(Connection.DB_USER_PROPERTY);//"admin";
		//String password = connProperties.getProperty(Connection.DB_PASSWORD_PROPERTY);//"admin";
		
		if (url.substring(0, 4).equals("http")){
			return new HttpConnection();
		}else{
			return new Connection();
		}
	}
	
	@Override
	public void close() throws OdaException {
		if(innerConnection!=null){
			innerConnection.close();
		}
		innerConnection=null;
	}

	@Override
	public void commit() throws OdaException {
		if(innerConnection!=null){
			innerConnection.commit();
		}
	}

	@Override
	public int getMaxQueries() throws OdaException {
		if(innerConnection!=null){
			return innerConnection.getMaxQueries();
		}
		return 1;
	}

	@Override
	public IDataSetMetaData getMetaData(String arg0) throws OdaException {
		if(innerConnection!=null){
			return innerConnection.getMetaData(arg0);
		}
		return new DataSetMetaData( this );
	}

	@Override
	public boolean isOpen() throws OdaException {
		if(innerConnection!=null){
			return innerConnection.isOpen();
		}
		return false;
	}

	@Override
	public IQuery newQuery(String arg0) throws OdaException {
		return innerConnection.newQuery(arg0);
	}

	@Override
	public void rollback() throws OdaException {
		innerConnection.rollback();
	}

	@Override
	public void setAppContext(Object arg0) throws OdaException {
		if(innerConnection!=null){
			innerConnection.setAppContext(arg0);
		}
	}

	@Override
	public void setLocale(ULocale arg0) throws OdaException {
		if(innerConnection!=null){
			innerConnection.setLocale(arg0);
		}
	}
	
}
