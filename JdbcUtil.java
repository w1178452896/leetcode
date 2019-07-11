package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @auther wangyaofeng
 * @date Aug 20, 20185:11:39 PM function
 */
public class JdbcUtil {

	/**
	 * 关闭连接
	 * 
	 * @param conn
	 * @param stm
	 * @param pstm
	 * @param rs
	 */
	public static void close(Connection conn, Statement stm,
			PreparedStatement pstm, ResultSet rs) {

		try {
			if (rs != null) {
				rs.close();
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if (pstm != null) {
				pstm.close();
			}
			} catch (Exception e2) {
				e2.printStackTrace();
			}finally{
				try {
					if (stm != null) {
						stm.close();
					}
				} catch (Exception e3) {
					e3.printStackTrace();
				}finally{
					try {
						if (conn != null) {
							conn.close();
						}
					} catch (Exception e4) {
						e4.printStackTrace();
					}
				}
			}		
			
		}
	}

	/**
	 * 获取list<Map<String,Object>>结果集
	 * 
	 * @param sql
	 * @param conn
	 * @return
	 */
	public static List<Map<String, Object>> sqlToList(String sql,
			Object[] params, Connection conn) {

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		PreparedStatement pstm = null;
		Statement stm = null;
		ResultSet rs = null;

		try {
			if (params == null || params.length == 0) {
				stm = conn.createStatement();
				rs = stm.executeQuery(sql);
			}else{
				pstm=conn.prepareStatement(sql);
				for (int i = 0; i < params.length; i++) {
					pstm.setObject(i+1, params[i]);
				}
				rs=pstm.executeQuery();
			}
			if(rs!=null){
				ResultSetMetaData md = rs.getMetaData();
				int columnCount = md.getColumnCount();
				while (rs.next()) {
					Map<String, Object> rowData = new HashMap<String, Object>();
					for (int i = 1; i <= columnCount; i++) {
						rowData.put(md.getColumnName(i), rs.getObject(i));
					}
					list.add(rowData);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			JdbcUtil.close(null, stm, pstm, rs);
		}

		return list;
	}

	/**
	 * 获取Map<String,Object>结果集
	 * 
	 * @param sql
	 * @param connection
	 * @return
	 */
	public static Map<String, Object> sqlToMap(String sql,Object[] params, Connection conn) {

		Map<String, Object> map = new HashMap<String, Object>();
		PreparedStatement pstm = null;
		Statement stm = null;
		ResultSet rs = null;

		try {
			if (params == null || params.length == 0) {
				stm = conn.createStatement();
				rs = stm.executeQuery(sql);
			}else{
				pstm=conn.prepareStatement(sql);
				for (int i = 0; i < params.length; i++) {
					pstm.setObject(i+1, params[i]);
				}
				rs=pstm.executeQuery();
			}
			if(rs!=null){
				ResultSetMetaData md = rs.getMetaData();
				int columnCount = md.getColumnCount();
				while (rs.next()) {
					for (int i = 1; i <= columnCount; i++) {
						map.put(md.getColumnName(i), rs.getObject(i));
					}		
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			JdbcUtil.close(null, stm, pstm, rs);
		}

		return map;

	}

	/**
	 * 查询sql，并把结果转为JSONArray
	 * 
	 * @param sql
	 * @param connection
	 * @return
	 */
	public static JSONArray sqlToJsonArray(String sql,Object[] params, Connection conn) {

		JSONArray jsonArray = new JSONArray();
		PreparedStatement pstm = null;
		Statement stm = null;
		ResultSet rs = null;

		try {
			if (params == null || params.length == 0) {
				stm = conn.createStatement();
				rs = stm.executeQuery(sql);
			}else{
				pstm=conn.prepareStatement(sql);
				for (int i = 0; i < params.length; i++) {
					pstm.setObject(i+1, params[i]);
				}
				rs=pstm.executeQuery();
			}
			if(rs!=null){
				ResultSetMetaData md = rs.getMetaData();
				int columnCount = md.getColumnCount();
				while (rs.next()) {
					JSONObject jsonObject = new JSONObject();
					for (int i = 1; i <= columnCount; i++) {
						jsonObject.put(md.getColumnName(i), rs.getObject(i));
					}
					jsonArray.put(jsonObject);
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
			//throw new RuntimeException(e.getMessage(),e);
		} finally {
			JdbcUtil.close(null, stm, pstm, rs);
		}

		return jsonArray;

	}

	/**
	 * 查询sql，并把结果转为JSONObject
	 * 
	 * @param sql
	 * @param connection
	 * @return
	 */
	public static JSONObject sqlToJsonObject(String sql,Object[] params, Connection conn) {

		JSONObject jsonObject = new JSONObject();
		PreparedStatement pstm = null;
		Statement stm = null;
		ResultSet rs = null;

		try {
			if (params == null || params.length == 0) {
				stm = conn.createStatement();
				rs = stm.executeQuery(sql);
			}else{
				pstm=conn.prepareStatement(sql);
				for (int i = 0; i < params.length; i++) {
					pstm.setObject(i+1, params[i]);
				}
				rs=pstm.executeQuery();
			}
			if(rs!=null){
				ResultSetMetaData md = rs.getMetaData();
				int columnCount = md.getColumnCount();
	
				while (rs.next()) {
					for (int i = 1; i <= columnCount; i++) {
						jsonObject.put(md.getColumnName(i), rs.getObject(i));
					}
				}
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
			//throw new RuntimeException(e.getMessage(),e);
		} finally {
			JdbcUtil.close(null, stm, pstm, rs);
		}

		return jsonObject;

	}
	
	
	public static Integer sqlOfUpdate(String sql,Object[] params, Connection conn){
		
		PreparedStatement pstm = null;
		Statement stm = null;
		int result=0;
		try {
			if (params == null || params.length == 0) {
				stm = conn.createStatement();
				result = stm.executeUpdate(sql);
			}else{
				pstm=conn.prepareStatement(sql);
				for (int i = 0; i < params.length; i++) {
					pstm.setObject(i+1, params[i]);
				}
				result=pstm.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			//throw new RuntimeException(e.getMessage(),e);
		}finally{
			JdbcUtil.close(null, stm, pstm, null);
		}
			
		return result;
	}
	/**
	 * 执行插入操作但不提交
	 * @param list
	 * @param table
	 * @param conn
	 * @throws SQLException
	 */
	private void insertBatch(List<Map<String, Object>> list,String table,Connection conn) throws SQLException{
		
		if(list==null||list.size()==0){
			return;
		}
		
		PreparedStatement pstm=null;
		StringBuilder sql = new StringBuilder();
		//拼接sql
		StringBuilder middleSql=new StringBuilder("values(");
		sql.append("insert into "+table+"(");
		Map<String, Object> one = list.get(0);
		Set<String> keySet = one.keySet();
		int i=0;
		for (String key : keySet) {
			if(i++!=0){
				sql.append(",");
				middleSql.append(",");
			}
			sql.append(key);
			middleSql.append("?");
		}
		middleSql.append(")");
		sql.append(") "+middleSql.toString());
		try {
			pstm = conn.prepareStatement(sql.toString());
			for (int j = 0; j < list.size(); j++) {
				Map<String, Object> map = list.get(j);
				i=1;
				for (String key : keySet) {
					pstm.setObject(i++, map.get(key));
				}
				pstm.addBatch();
				if(j!=0 && j%50==0){ //50条数据执行一次
					pstm.executeBatch();
				}
			}
			pstm.executeBatch();
		}finally{
			CoalIndicatorCalcUtil.closeConn(null, pstm, null);
		}
	
	}
	
	
	
	
	
	
}
