package org.orienteer.birt.orientdb.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import javax.xml.bind.DatatypeConverter;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.SortSpec;
import org.eclipse.datatools.connectivity.oda.spec.QuerySpecification;

import com.orientechnologies.orient.core.record.impl.ODocument;

public class HttpQuery implements IQuery{

	private ODocument dbResult;
	private ResultSetMetaData curMetaData;
	private ResultSet curResultSet;
	private int m_maxRows;
	private String queryText;
	private HttpConnection connection;

	public HttpQuery(HttpConnection connection){
		this.connection = connection;
	}
	
	@Override
	public void prepare(String arg0) throws OdaException {

		Properties connProperties = connection.getProperties();
		String url = connProperties.getProperty(Connection.DB_URI_PROPERTY);//"remote:127.0.0.1/Orienteer";
		String username = connProperties.getProperty(Connection.DB_USER_PROPERTY);//"admin";
		String password = connProperties.getProperty(Connection.DB_PASSWORD_PROPERTY);//"admin";

		try {
		
			URL obj = new URL(url+arg0);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
	
			con.setRequestMethod("GET");
	
			con.setRequestProperty("User-Agent", "Mozilla/5.0");
			con.setRequestProperty("accept", "application/json");
			con.setRequestProperty("Authorization", "Basic " + DatatypeConverter.printBase64Binary((username + ":" + password).getBytes()));

	
			//int responseCode = con.getResponseCode();
	
			BufferedReader in;
				in = new BufferedReader(
				        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
	
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
	
			String out = response.toString();
			dbResult = new ODocument();
			dbResult.fromJSON(out);
			List<ODocument> resultList = dbResult.field("result");
			curMetaData = new ResultSetMetaData(resultList.get(0));
			curResultSet = new ResultSet(resultList,curMetaData);
		
		} catch (IOException e) {
			throw new OdaException(e);
		}

	}
	
	
	
	@Override
	public IResultSet executeQuery() throws OdaException {
		// TODO Auto-generated method stub
		return curResultSet;
	}

	@Override
	public void close() throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cancel() throws OdaException, UnsupportedOperationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearInParameters() throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int findInParameter(String arg0) throws OdaException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getEffectiveQueryText() {
		return queryText;
	}

	@Override
	public int getMaxRows() throws OdaException {
		return m_maxRows;
	}

	@Override
	public IResultSetMetaData getMetaData() throws OdaException {
		// TODO Auto-generated method stub
		return curMetaData;
	}

	@Override
	public IParameterMetaData getParameterMetaData() throws OdaException {
		// TODO Auto-generated method stub
		return new ParameterMetaData();
	}

	@Override
	public SortSpec getSortSpec() throws OdaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QuerySpecification getSpecification() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void setAppContext(Object arg0) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBigDecimal(String arg0, BigDecimal arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBigDecimal(int arg0, BigDecimal arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBoolean(String arg0, boolean arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBoolean(int arg0, boolean arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDate(String arg0, Date arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDate(int arg0, Date arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDouble(String arg0, double arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDouble(int arg0, double arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInt(String arg0, int arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInt(int arg0, int arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMaxRows(int arg0) throws OdaException {
		m_maxRows = arg0;
	}

	@Override
	public void setNull(String arg0) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNull(int arg0) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setObject(String arg0, Object arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setObject(int arg0, Object arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setProperty(String arg0, String arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setSortSpec(SortSpec arg0) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setSpecification(QuerySpecification arg0) throws OdaException, UnsupportedOperationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setString(String arg0, String arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setString(int arg0, String arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTime(String arg0, Time arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTime(int arg0, Time arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTimestamp(String arg0, Timestamp arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTimestamp(int arg0, Timestamp arg1) throws OdaException {
		// TODO Auto-generated method stub
		
	}

}
