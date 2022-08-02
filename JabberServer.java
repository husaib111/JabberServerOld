//husaibhasan2170223
package com.bham.fsd.assignments.jabberserver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;

public class JabberServer {
	
	private static String dbcommand = "jdbc:postgresql://127.0.0.1:5432/postgres";
	private static String db = "postgres";
	private static String pw = "";

	private static Connection conn;
	
	public static Connection getConnection() {
		return conn;
	}

	public static void main(String[] args) {
				
		JabberServer jabber = new JabberServer();
		JabberServer.connectToDatabase();
		jabber.resetDatabase(); 	
		
		//jabber.getFollowerUserIDs(11);
		//jabber.getFollowingUserIDs(11);
		//jabber.getLikesOfUser(0);
		//jabber.addUser("john", "john@hotm.com");
		//jabber.addJab("john", "aamir ali says hi");
		//jabber.addFollower(11, 13);
		//jabber.addLike(5, 8);
		//jabber.getUsersWithMostFollowers();
		//jabber.getMutualFollowUserIDs();

	}
	
	//This method returns a list of the userids (as Strings) of the jabberusers that follow the user with the userid userid.
	
	public ArrayList<String> getFollowerUserIDs(int userid) {
		ArrayList<String> followerUserIDs = new ArrayList<String>();
		
		String query = "SELECT useridA FROM follows WHERE useridB = ?;";
		query = query.replace("?", String.valueOf(userid));

		try {
			PreparedStatement pstmt = conn.prepareStatement(query);
			
			ResultSet rs = pstmt.executeQuery();

			while (rs.next()) {
				followerUserIDs.add(rs.getObject("useridA").toString()); 
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < followerUserIDs.size(); i++) {
			System.out.println(followerUserIDs.get(i));
		}
		return followerUserIDs;
	}
	
//This method returns a list of the userids of the jabberusers that the user with the userid userid is following. 

	public ArrayList<String> getFollowingUserIDs(int userid) {
		ArrayList<String> followingUserIDs = new ArrayList<String>();
		
		String query = "SELECT useridB FROM follows WHERE useridA = ?;";
		query = query.replace("?", String.valueOf(userid));

		try {
			PreparedStatement pstmt = conn.prepareStatement(query);
			
			ResultSet rs = pstmt.executeQuery();

			while (rs.next()) {
				followingUserIDs.add(rs.getObject("useridB").toString()); 
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < followingUserIDs.size(); i++) {
			System.out.println(followingUserIDs.get(i));
		}
		return followingUserIDs;
	}
	
//  This method returns a list of ArrayLists, each containing a pair of ‘mutual follows’. A mutual follow is
//	when user A follows user B AND user B follows user A. The userids are to be returned. There should
//	be no duplicate follows relationships in the returned list. By definition, if there exists a mutual follows
//	relationship between user A and user B then user A follows user B and user B follows user B. The returned
//	list should contain only one instance of this relationship between user A and user B, i.e. it should return
//	only {[userA, userB]} not {[userA, userB],[userB, userA]}.
	
	public ArrayList<ArrayList<String>> getMutualFollowUserIDs() {
		ArrayList<ArrayList<String>> mutualFollowers = new ArrayList<ArrayList<String>>();

		try {

			PreparedStatement stmt = conn.prepareStatement("SELECT a.useridA, a.useridB FROM follows as a INNER JOIN follows as b ON a.useridA = b.useridB AND a.useridB = b.useridA;");
		
			
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ArrayList<String> r = new ArrayList<String>();
				r.add(rs.getObject("useridA").toString());
				r.add(rs.getObject("useridB").toString());
				mutualFollowers.add(r);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int halfwayMark = mutualFollowers.size() / 2;
		mutualFollowers.subList(halfwayMark, mutualFollowers.size()).clear();
		for (int i = 0; i < mutualFollowers.size(); i++) {
			System.out.println(mutualFollowers.get(i));
		}

		return mutualFollowers;

	}
	
//  This method returns a list of ArrayLists with the username and jabtext of all jabs liked by the user userid. 
//	The username is the username of the user who posted the jab. Thus, each of the ArrayLists
//	returned will have two elements: {username, jabtext}.

	public ArrayList<ArrayList<String>> getLikesOfUser(int userid) {
		ArrayList<ArrayList<String>> likes = new ArrayList<ArrayList<String>>();
		
		String query = "SELECT DISTINCT username, jabtext FROM jabberuser, jab, likes WHERE  (jabberuser.userid = 0 AND likes.userid = 0) AND (jab.userid = jabberuser.userid)";
		query = query.replaceAll("0", String.valueOf(userid));

		try {
			PreparedStatement pstmt = conn.prepareStatement(query);
			
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				ArrayList<String> r = new ArrayList<String>();
				r.add(rs.getObject("username").toString());
				r.add(rs.getObject("jabtext").toString());
				likes.add(r);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < likes.size(); i++) {
			System.out.println(likes.get(i));
		}

		return likes;

	}
	
	//This method returns the ‘timeline’ of a user. A user’s timeline is all of the jabs posted by users they follow.
	//Each row of the result should be the username of the user who posted the jab and the jab text. Thus,
	//each of the ArrayLists returned will have two elements: {username, jabtext}.
	
	public ArrayList<ArrayList<String>> getTimelineOfUser(int userid) {
		ArrayList<ArrayList<String>> ret = new ArrayList<ArrayList<String>>();

		try {

			PreparedStatement stmt = conn.prepareStatement("SELECT DISTINCT username, jabtext FROM jabberuser NATURAL JOIN jab natural join (SELECT useridB FROM follows WHERE useridA = ?) AS a1;");
		
			stmt.setInt(1, userid);
			
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ArrayList<String> r = new ArrayList<String>();
				r.add(rs.getObject("displayname").toString());
				r.add(rs.getObject("groupname").toString());
				r.add(rs.getObject("messagetext").toString());
				ret.add(r);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ret;

	}
	
	//This method adds a new jab to the jab table. The user is represented by username and the jab by jabtext.
	//You must find the userid of the user with the username username to do this. You will need to generate a
	//new jabid number that does not already exist in the table.

	public void addJab(String username, String jabtext) {
		ArrayList<Integer> useridd = new ArrayList<Integer>();
		
		String query = "SELECT userid FROM jabberuser WHERE username = " + "\'" + username + "\'";

		try {
			PreparedStatement pstmt = conn.prepareStatement(query);
			
			ResultSet rse = pstmt.executeQuery();

			while (rse.next()) {
				useridd.add(rse.getInt("userid")); 
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("the userid = " + useridd.get(0));
		
		int newid = getNextJabID();
		
		try {
			
			PreparedStatement stmt = conn.prepareStatement("insert into jab values(?,?,?)");
			System.out.println(stmt);
			stmt.setInt(1, newid);
			stmt.setInt(2, useridd.get(0));
			stmt.setString(3, jabtext);
			System.out.println(stmt);
			stmt.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	private int getNextJabID() {
		
		String query = "select max(jabid) from jab";
		
		int maxid = -1;
		
		try (Statement stmt = conn.createStatement()) {
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				maxid = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if (maxid < 0) {
			return maxid;
		}
		
		return maxid + 1;
	}
	
	//This method adds a new user to the jabberuser table with username username and email address emailadd.
	//You will need to generate a new userid number that does not already exist in the table.
	
	public void addUser(String username, String emailadd) {
		
		int nextID = getNextUserID();
		
		try {

			PreparedStatement stmt = conn.prepareStatement("insert into jabberuser (values(?,?,?))");
			

			stmt.setInt(1,  nextID);
			stmt.setString(2, username);
			stmt.setString(3, emailadd);
			
			
			stmt.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	private int getNextUserID() {
		
		String query = "select max(userid) from jabberuser";
		
		int maxid = -1;
		
		try (Statement stmt = conn.createStatement()) {
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				maxid = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if (maxid < 0) {
			return maxid;
		}
		
		return maxid + 1;
	}
	
	//This method adds a new follows relationship: userida follows useridb.
	public void addFollower(int userida, int useridb) {
		
		try {

			PreparedStatement stmt = conn.prepareStatement("insert into follows (values(?,?))");
			

			stmt.setInt(1, userida);
			stmt.setInt(2, useridb);
			
			
			stmt.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	//This method adds a new like: user userid likes jab jabid
	
	public void addLike(int userid, int jabid) {
		
		try {

			PreparedStatement stmt = conn.prepareStatement("insert into likes (values(?,?))");
			

			stmt.setInt(1, userid);
			stmt.setInt(2, jabid);
			
			
			stmt.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//This method returns the userids of the user(s) with the most followers. This might be more than one user
	//if more than one user has an equally high number of followers.
	
	public ArrayList<String> getUsersWithMostFollowers() {
		ArrayList<String> mostFollowers = new ArrayList<String>();
		
		try {
			
			PreparedStatement stmt = conn.prepareStatement("SELECT useridB FROM follows group by useridB having count(useridB) >= all (select count(useridB) from follows group by useridB order by count(useridB) desc);");
			
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				mostFollowers.add(rs.getObject("useridB").toString());
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(mostFollowers.get(0));
		
	
		return mostFollowers;		

	}
	
	public JabberServer() {}
	
	public static void connectToDatabase() {

		try {
			conn = DriverManager.getConnection(dbcommand,db,pw);

		}catch(Exception e) {		
			e.printStackTrace();
		}
	}

	/*
	 * Utility method to print an ArrayList of ArrayList<String>s to the console.
	 */
	private static void print2(ArrayList<ArrayList<String>> list) {
		
		for (ArrayList<String> s: list) {
			print1(s);
			System.out.println();
		}
	}
		
	/*
	 * Utility method to print an ArrayList to the console.
	 */
	private static void print1(ArrayList<String> list) {
		
		for (String s: list) {
			System.out.print(s + " ");
		}
	}

	public void resetDatabase() {
		
		dropTables();
		
		ArrayList<String> defs = loadSQL("jabberdef");
	
		ArrayList<String> data =  loadSQL("jabberdata");
		
		executeSQLUpdates(defs);
		executeSQLUpdates(data);
	}
	
	private void executeSQLUpdates(ArrayList<String> commands) {
	
		for (String query: commands) {
			
			try (PreparedStatement stmt = conn.prepareStatement(query)) {
				stmt.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private ArrayList<String> loadSQL(String sqlfile) {
		
		ArrayList<String> commands = new ArrayList<String>();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(sqlfile + ".sql"));
			
			String command = "";
			
			String line = "";
			
			while ((line = reader.readLine())!= null) {
				
				if (line.contains(";")) {
					command += line;
					command = command.trim();
					commands.add(command);
					command = "";
				}
				
				else {
					line = line.trim();
					command += line + " ";
				}
			}
			
			reader.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return commands;
		
	}

	private void dropTables() {
		
		String[] commands = {
				"drop table jabberuser cascade;",
				"drop table jab cascade;",
				"drop table follows cascade;",
				"drop table likes cascade;"};
		
		for (String query: commands) {
			
			try (PreparedStatement stmt = conn.prepareStatement(query)) {
				stmt.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}

