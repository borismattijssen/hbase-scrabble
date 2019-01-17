import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class HBaseScrabble {
	private Configuration config;
	private HBaseAdmin hBaseAdmin;

	private byte[] table = Bytes.toBytes("Games");
	private byte[] infoFamily = Bytes.toBytes("d");
	private byte[] winnerFamily = Bytes.toBytes("w");
	private byte[] loserFamily = Bytes.toBytes("l");

	/**
	 * The Constructor. Establishes the connection with HBase.
	 * @param zkHost
	 * @throws IOException
	 */
	public HBaseScrabble(String zkHost) throws IOException {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
		config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
		HBaseConfiguration.addHbaseResources(config);
		this.hBaseAdmin = new HBaseAdmin(config);
	}

	public void createTable() throws IOException {
		HTableDescriptor table = new HTableDescriptor(this.table);
		HColumnDescriptor infoFamily = new HColumnDescriptor(Bytes.toBytes("d"));
		HColumnDescriptor winnerFamily = new HColumnDescriptor(Bytes.toBytes("w"));
		HColumnDescriptor loserFamily = new HColumnDescriptor(Bytes.toBytes("l"));
		table.addFamily(infoFamily);
		table.addFamily(winnerFamily);
		table.addFamily(loserFamily);
		hBaseAdmin.createTable(table);
	}

	public void loadTable(String folder)throws IOException{
		List<Path> files = Files.walk(Paths.get(folder))
				.filter(Files::isRegularFile)
				.collect(Collectors.toList());

		HConnection conn = HConnectionManager.createConnection(this.config);
		HTable table = new HTable(TableName.valueOf("Games"),conn);

		for(Path file : files) {
			Reader reader = Files.newBufferedReader(file);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
			for (CSVRecord csvRecord : csvParser) {
				ArrayList<String> values = new ArrayList<>();
				csvRecord.iterator().forEachRemaining(values::add);


				String tourneyIdKey = values.get(1);
				while (tourneyIdKey.length() <= 10) tourneyIdKey = "0" + tourneyIdKey;
				String gameIdKey = values.get(0);
				while (gameIdKey.length() <= 10) gameIdKey = "0" + gameIdKey;

				byte[] key = getKey(new String[]{tourneyIdKey, gameIdKey}, new int[]{0, 1});
				Put put = new Put(key);

				put.add(infoFamily, Bytes.toBytes("gid"), Bytes.toBytes(csvRecord.get(0)));
				put.add(infoFamily, Bytes.toBytes("tid"), Bytes.toBytes(csvRecord.get(1)));
				put.add(infoFamily, Bytes.toBytes("tie"), Bytes.toBytes(csvRecord.get(2)));
				put.add(infoFamily, Bytes.toBytes("rnd"), Bytes.toBytes(csvRecord.get(15)));
				put.add(infoFamily, Bytes.toBytes("div"), Bytes.toBytes(csvRecord.get(16)));
				put.add(infoFamily, Bytes.toBytes("date"), Bytes.toBytes(csvRecord.get(17)));
				put.add(infoFamily, Bytes.toBytes("lex"), Bytes.toBytes(csvRecord.get(18)));

				put.add(winnerFamily, Bytes.toBytes("id"), Bytes.toBytes(csvRecord.get(3)));
				put.add(winnerFamily, Bytes.toBytes("name"), Bytes.toBytes(csvRecord.get(4)));
				put.add(winnerFamily, Bytes.toBytes("score"), Bytes.toBytes(csvRecord.get(5)));
				put.add(winnerFamily, Bytes.toBytes("or"), Bytes.toBytes(csvRecord.get(6)));
				put.add(winnerFamily, Bytes.toBytes("nr"), Bytes.toBytes(csvRecord.get(7)));
				put.add(winnerFamily, Bytes.toBytes("pos"), Bytes.toBytes(csvRecord.get(8)));

				put.add(loserFamily, Bytes.toBytes("id"), Bytes.toBytes(csvRecord.get(9)));
				put.add(loserFamily, Bytes.toBytes("name"), Bytes.toBytes(csvRecord.get(10)));
				put.add(loserFamily, Bytes.toBytes("score"), Bytes.toBytes(csvRecord.get(11)));
				put.add(loserFamily, Bytes.toBytes("or"), Bytes.toBytes(csvRecord.get(12)));
				put.add(loserFamily, Bytes.toBytes("nr"), Bytes.toBytes(csvRecord.get(13)));
				put.add(loserFamily, Bytes.toBytes("pos"), Bytes.toBytes(csvRecord.get(14)));


				table.put(put);
			}
		}
	}

	/**
	 * This method generates the key
	 * @param values The value of each column
	 * @param keyTable The position of each value that is required to create the key in the array of values.
	 * @return The encoded key to be inserted in HBase
	 */
	private byte[] getKey(String[] values, int[] keyTable) {
		String[] keyValues = new String[keyTable.length];
		for (int i = 0; i < keyTable.length; i++) {
		   keyValues[i] = values[keyTable[i]];
		}
		byte[] key = Bytes.toBytes(String.join(":", keyValues));

		return key;
	}


	// Returns all the opponents (Loserid) of a given Winnername in a tournament (Tourneyid).
	public List<String> query1(String tourneyid, String winnername) throws IOException {

		while (tourneyid.length() <= 10) tourneyid = "0" + tourneyid;
		
		// filter for winnername
		Filter f = new SingleColumnValueFilter(
				winnerFamily,
				Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(winnername));
		
		
		// create scanner with filters
		Scan scan = new Scan()
			.setRowPrefixFilter(Bytes.toBytes(tourneyid + ":"))
			.setFilter(f);

		HTable hTable = new HTable(this.config, this.table);
		ResultScanner rs = hTable.getScanner(scan);

		List<String> losers = new ArrayList<>();

		Result result = rs.next();
		while (result!=null && !result.isEmpty()){
			String loserId = Bytes.toString(result.getValue(loserFamily, Bytes.toBytes("id")));
			losers.add(loserId);
			result = rs.next();
		}
		return losers;
	}

	// Returns the ids of the players (winner and loser) that have participated more than once
	// in all tournaments between two given Tourneyids.
	public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {

		while (firsttourneyid.length() <= 10) firsttourneyid = "0" + firsttourneyid;
        while (lasttourneyid.length() <= 10) lasttourneyid = "0" + lasttourneyid;

		byte[] start = Bytes.toBytes(firsttourneyid + ":0000000000");
        byte[] end = Bytes.toBytes(lasttourneyid + ":0000000000");
		
		HTable hTable = new HTable(this.config, this.table);

		Scan scan = new Scan(start,end);
		ResultScanner rs = hTable.getScanner(scan);
				
		// Logic: for each tourney, the set of players appeared twice is saved in twiceCurrent.
		// Only the players appeared twice in the previous tourney are considered during the check.
		// When the current tourney changes, the twiceCurrent set became the twiceOld set for the new tourney.
		// In the last iteration, in the twiceCurrent set, there will be all those players "survived".
		// For the first tourney the player do not need to appear in TwiceOld, for obvious reason.
		HashSet<String> twiceOld = new HashSet<>();
		HashSet<String> firstCurrent = new HashSet<>();
		HashSet<String> twiceCurrent = new HashSet<>();

		int firstTourneyInt = Integer.parseInt(firsttourneyid);
		int currentTourney = Integer.parseInt(firsttourneyid);

		Result result = rs.next();

		while (result!=null && !result.isEmpty()) {
			
			String idWinnerString = Bytes.toString(result.getValue(winnerFamily,Bytes.toBytes("id")));
			String idLoserString = Bytes.toString(result.getValue(loserFamily,Bytes.toBytes("id")));
			int tourneyId = Integer.parseInt(Bytes.toString(result.getValue(infoFamily,Bytes.toBytes("tid"))));
			
			if (tourneyId != currentTourney) {
				firstCurrent = new HashSet<>();
				twiceOld = twiceCurrent;
				twiceCurrent = new HashSet<>();
				currentTourney = tourneyId;
			}

			if (twiceOld.contains(idWinnerString) || currentTourney==firstTourneyInt) {
				if (firstCurrent.contains(idWinnerString)) twiceCurrent.add(idWinnerString);
				else firstCurrent.add(idWinnerString);
			}

			if (twiceOld.contains(idLoserString) || currentTourney==firstTourneyInt) {
				if (firstCurrent.contains(idLoserString)) twiceCurrent.add(idLoserString);
				else firstCurrent.add(idLoserString);
			}

			result = rs.next();
		}

		return (new ArrayList<>(twiceCurrent));
	}


	// Given a Tourneyid, the query returns the Gameid, the ids of the two
	// participants that have finished in tie.
	public List<String> query3(String tourneyid) throws IOException {

		while (tourneyid.length() <= 10) tourneyid = "0" + tourneyid;

		// filter for ties
		Filter f = new SingleColumnValueFilter(
				infoFamily,
				Bytes.toBytes("tie"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("True"));

		// create scanner with filters
		Scan scan = new Scan()
			.setRowPrefixFilter(Bytes.toBytes(tourneyid + ":"))
			.setFilter(f);

		HTable hTable = new HTable(this.config, this.table);
		ResultScanner rs = hTable.getScanner(scan);

		List<String> games = new ArrayList<>();

		Result result = rs.next();
		while (result!=null && !result.isEmpty()){
			String winnerId = Bytes.toString(result.getValue(winnerFamily, Bytes.toBytes("id")));
			String loserId = Bytes.toString(result.getValue(loserFamily, Bytes.toBytes("id")));
			String gameId = Bytes.toString(result.getValue(infoFamily, Bytes.toBytes("gid")));
			games.add(gameId + ";" + winnerId + ";" + loserId);
			result = rs.next();
		}
		return games;
	}


	public static void main(String[] args) throws IOException {
		if(args.length<2){
			System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
					"\ta) If loadTable: csvsFolder.\n " +
					"\tb) If query1: tourneyid winnername.\n  " +
					"\tc) If query2: firsttourneyid lasttourneyid.\n  " +
					"\td) If query3: tourneyid.\n  ");
			System.exit(-1);
		}
		HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
		if(args[1].toUpperCase().equals("CREATETABLE")){
			hBaseScrabble.createTable();
		}
		else if(args[1].toUpperCase().equals("LOADTABLE")){
			if(args.length!=3){
				System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
				System.exit(-1);
			}
			else if(!(new File(args[2])).isDirectory()){
				System.out.println("Error: Folder "+args[2]+" does not exist.");
				System.exit(-2);
			}
			hBaseScrabble.loadTable(args[2]);
		}
		else if(args[1].toUpperCase().equals("QUERY1")){
			if(args.length!=4){
				System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
						"3) tourneyid 4) winnername");
				System.exit(-1);
			}

			List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
			System.out.println("There are "+opponentsName.size()+" opponents of winner "+args[3]+" that play in tourney "+args[2]+".");
			System.out.println("The list of opponents is: "+Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
		}
		else if(args[1].toUpperCase().equals("QUERY2")){
			if(args.length!=4){
				System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
						"3) firsttourneyid 4) lasttourneyid");
				System.exit(-1);
			}
			List<String> playerNames =hBaseScrabble.query2(args[2], args[3]);
			System.out.println("There are "+playerNames.size()+" players that participates in more than one tourney between tourneyid "+args[2]+" and tourneyid "+args[3]+" .");
			System.out.println("The list of players is: "+Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
		}
		else if(args[1].toUpperCase().equals("QUERY3")){
			if(args.length!=3){
				System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
						"3) tourneyid");
				System.exit(-1);
			}
			List<String> games = hBaseScrabble.query3(args[2]);
			System.out.println("There are "+games.size()+" that ends in tie in tourneyid "+args[2]+" .");
			System.out.println("The list of games is: "+Arrays.toString(games.toArray(new String[games.size()])));
		}
		else{
			System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
					"\ta) If loadTable: csvsFolder.\n " +
					"\tb) If query1: tourneyid winnername.\n  " +
					"\tc) If query2: firsttourneyid lasttourneyid.\n  " +
					"\td) If query3: tourneyid.\n  ");
			System.exit(-1);
		}

	}



}
