import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.swing.*;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.client.Get; 
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.Put; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner; 
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.util.Bytes;

/*
 * for logging in, the user names are user(1-500)@asu.edu
 */
public class MessageSystem {
	
	private static String user = "";
	
	public static void main (String[] args) throws IOException {
		// Main frames for GUI
		JTabbedPane userPage = new JTabbedPane();
        	JFrame frame         = new JFrame("Messaging App");
        	frame.setPreferredSize(new Dimension(960,720));
        	ArrayList<String> localMessages = new ArrayList<>();
        
        	// Login tab for GUI
        	JComponent selectUser = makeTextPanel("Login");
		JButton login         = new JButton("Login");
		JButton close         = new JButton("Close Table");
		JButton init          = new JButton("Initilize Data");
		JLabel log            = new JLabel("Login:");
		JTextArea userInfo    = new JTextArea(5,20);
		userInfo.setEditable(true);
		
		init.setBounds(390, 220, 160, 25);
		userInfo.setBounds(343,160,250,25);
		log.setBounds(295,160,90,25);
		login.setBounds(420,190,100,25);
		close.setBounds(650,190,160,25);
		userPage.addTab("Select User", selectUser);
		
		selectUser.add(login);
		selectUser.add(init);
		selectUser.add(log);
		selectUser.add(userInfo);
		selectUser.add(close);

		// Received Messages for GUI
		JComponent receivedMessages  = makeTextPanel("Received");
		JButton receivedRefresh      = new JButton("Refresh");
		JTextArea messageBoxReceiver = new JTextArea();
		JScrollPane scrollReceiver   = new JScrollPane(messageBoxReceiver);
		
		receivedRefresh.setBounds(750,0,160,25);
		receivedMessages.add(receivedRefresh);
		
		// Sent Messages for GUI
		JComponent sentMessages    = makeTextPanel("Sent");
		JButton sentRefresh        = new JButton("Refresh");
		JTextArea messageBoxSender = new JTextArea();
		JScrollPane scrollSender   = new JScrollPane(messageBoxSender);
		
		sentRefresh.setBounds(750,0,160,25);
		sentMessages.add(sentRefresh);
		
		// Send Messages for GUI
		JComponent sendTab  = makeTextPanel("Send");
		JButton send        = new JButton("Send");
		JLabel recipUser    = new JLabel("To: ");
		JLabel messageTitle = new JLabel("Title: ");
		JLabel body         = new JLabel("Message: ");
		
		send.setBounds(0, 450, 100, 25);
		recipUser.setBounds(0, 0, 50, 25);
		messageTitle.setBounds(0, 25, 50, 25);
		body.setBounds(0, 50, 75, 25);
		
		JTextArea toUser = new JTextArea(); // text area for recip
		toUser.setVisible(true);
		toUser.setBounds(37, 5, 900, 15);
		
		JTextArea title = new JTextArea(); // text area for title
		title.setVisible(true);
		title.setBounds(37, 30, 900, 15);
		
		JTextArea messForSend = new JTextArea(); // text area for body
		messForSend.setVisible(true);
		messForSend.setBounds(37, 70, 900, 300);
		
		sendTab.add(send);
		sendTab.add(recipUser);
		sendTab.add(messageTitle);
		sendTab.add(body);
		sendTab.add(toUser);
		sendTab.add(title);
		sendTab.add(messForSend);
		
		// generates random dates to add to the data set
		LocalDate earliestDate = LocalDate.of(2010, 1, 1);
		long earliest          = earliestDate.toEpochDay();
		
		LocalDate lastestDate = LocalDate.now();
		long lastest          = lastestDate.toEpochDay();
		
		SimpleDateFormat sdf      = new SimpleDateFormat("HH:mm:ss");
    		SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd");
	    	
		// creates HBase table
		Configuration config = HBaseConfiguration.create();
		HTable table         = new HTable(config, "messagelogs");
		
		// ActionListener to initialize data
		ActionListener runData = new ActionListener() { 

			public void actionPerformed(ActionEvent e) { 
				System.out.println("HBase Start");
				
				// generates 500 users
				for (int j = 0; j < 500; j++) { 
				    int i = 0;
			
				    String userName = generateUser(j);
				    	
				    // sends 10 messages per user
				    while (i < 10) { 	
				    	long randomDate = ThreadLocalRandom.current().longs(earliest, lastest)
				    			.findAny().getAsLong(); 
					    
					String sDate = LocalDate.ofEpochDay(randomDate).toString();
						
					Long randomTime = new Random().nextLong();
					String sTime    = sdf.format(randomTime).toString();
				    	
					String recipUser = "";
					int randInt      = (int)(Math.random()*500);
					    
					if (randInt != j) {
					    recipUser = generateUser(randInt); 
					} else {
					    randInt = (int)(Math.random()*500);
					}

				    	Put p = new Put(Bytes.toBytes(recipUser + " " + sDate + " " + sTime));
					    	
				    	p.add(Bytes.toBytes("message"), Bytes.toBytes("author"), Bytes.toBytes(userName));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("recip"), Bytes.toBytes(recipUser));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("title"), Bytes.toBytes("Greetings!"));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("body"), Bytes.toBytes("Hello World!"));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("date"), Bytes.toBytes(sDate));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("time"), Bytes.toBytes(sTime));
					    
					try {
						table.put(p);
					} catch (IOException e1) {}
					    	
					i++;
				    }
				}	 
			}
		};
			
		// ActionListener for login, this will also be used for received and sent messages.
		ActionListener userLogin = new ActionListener() { 

			public void actionPerformed(ActionEvent e) {
				user = userInfo.getText().trim().toLowerCase();
				if (!user.contains("@asu.edu")) {
					user += "@asu.edu";
				}
				
				// generates the tabs for received, send, and sent
				userPage.addTab("Received Messages", receivedMessages);
				userPage.addTab("Send Message", sendTab);
				userPage.addTab("Sent Messages", sentMessages);
				
				messageBoxSender.setEditable(false);
				messageBoxSender.setSize(900, 100);
				messageBoxSender.setVisible(true);
				
				messageBoxSender.setPreferredSize(new Dimension(500,500));
				scrollSender.setVisible(true);
				scrollSender.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);

				messageBoxReceiver.setEditable(false);
				messageBoxReceiver.setSize(900, 100);
				messageBoxReceiver.setVisible(true);
				sentMessages.setLayout(new FlowLayout());
				sentMessages.add(scrollSender);
				
				scrollReceiver.setPreferredSize(new Dimension(500,500));
				scrollReceiver.setVisible(true);
				scrollReceiver.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);

				// begins range scan and puts them into the received tab
				ArrayList<String> arr = rangeScan(table, user); // ln 355
				
				for(String entry : arr) {
					messageBoxReceiver.append(entry);
					receivedMessages.setLayout(new FlowLayout());
					receivedMessages.add(scrollReceiver);
				}
			}
		};
		
		// ActionListener for sending messages to other users
		ActionListener sendMessage = new ActionListener() { 

			public void actionPerformed(ActionEvent e) { 
				String user       = userInfo.getText().trim();
				String sTitle     = title.getText();
				String sRecipUser = toUser.getText();
				
				String[] multiUsers = sRecipUser.split(",");
				String sMessageBox  = messForSend.getText();
				
				Date today = new Date();
						
				String sDate = dateForm.format(today);
				String sTime = sdf.format(today).toString();
				
				// gets every user in the recip text box and sends each one a message
				for(int i = 0; i < multiUsers.length; i++) { 
					String recipUser = multiUsers[i].trim();
					
					Put p = new Put(Bytes.toBytes(recipUser + " "+ sDate + " " + sTime));
						    	
					p.add(Bytes.toBytes("message"), Bytes.toBytes("author"), Bytes.toBytes(user));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("recip"), Bytes.toBytes(recipUser));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("title"), Bytes.toBytes(sTitle));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("body"), Bytes.toBytes(sMessageBox));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("date"), Bytes.toBytes(sDate));
					p.add(Bytes.toBytes("message"), Bytes.toBytes("time"), Bytes.toBytes(sTime));
					
					localMessages.add("Message to: " + recipUser + " \nTitle: " + sTitle 
							+ " \nMessage body: " + sMessageBox + " \nsent: " + sDate + " at " + sTime + "\n\n");
					try {
						table.put(p);
					} catch (IOException e1) {}
					
					toUser.setText("");
					title.setText("");
					messForSend.setText("");
				}
			}
		};
		
		// ActionListener to refresh messages
		ActionListener refreshMessage = new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				// checks to see which refresh button was pressed
				if (e.getSource() == receivedRefresh) {
					messageBoxReceiver.setText("");
					
					// runs another range scan to retreive messages
					ArrayList<String> arr = rangeScan(table, user); // ln 355
					
					for(String entry : arr) {
						messageBoxReceiver.append(entry);
					}
					receivedMessages.setLayout(new FlowLayout());
					receivedMessages.add(scrollReceiver);
	
				} else {
					while (!localMessages.isEmpty()) {
						messageBoxSender.append(localMessages.get(0));
						localMessages.remove(localMessages.get(0));
					}
				}
			}
		};
		
		// ActionListener to close the HBase table
		ActionListener closeTable = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					table.close();
				} catch (IOException e1) {
				}

				System.out.println("HBase End");	
			} 	
		};
		
		// sets each button to an ActionListener
		login.addActionListener(userLogin);
		init.addActionListener(runData); 
		send.addActionListener(sendMessage);
		close.addActionListener(closeTable);
		receivedRefresh.addActionListener(refreshMessage);
		sentRefresh.addActionListener(refreshMessage);
		
		// Closes GUI
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.add(userPage, BorderLayout.CENTER);
		frame.pack();
		frame.setVisible(true);
	}
	
	// generates users names and adds 0 depending on the i that is passed in.
	public static String generateUser(int i) { 
		String user = "";
		if (i < 10) {
		    user = "user00" + (i+1) + "@asu.edu";
		} else if (i < 100) {
		   user = "user0" + (i+1) + "@asu.edu";
		} else {
		    user = "user" + (i+1) + "@asu.edu";
		}
		return user;
	}
	
	// creates the panels for each tab.
	public static JComponent makeTextPanel(String text) {
	    JPanel panel = new JPanel(false);
	    JLabel filler = new JLabel(text);
	    filler.setHorizontalAlignment(JLabel.CENTER);
	    panel.setLayout(null);
	    panel.add(filler);
	    return panel;
	}
	
	// sets a range for the rangeScan.
	public static Scan scannerTable(String userName) {
	    Scan s = new Scan();
	    s.setStartRow(Bytes.toBytes(userName + " 2009-12-31 23:59:59"));
	    s.setStopRow(Bytes.toBytes(userName + " 2022-01-01 00:00:00"));
	    
	    s.addColumn(Bytes.toBytes("message"), Bytes.toBytes("recip"));
	    s.addColumn(Bytes.toBytes("message"), Bytes.toBytes("author"));
	    s.addColumn(Bytes.toBytes("message"), Bytes.toBytes("title"));
	    s.addColumn(Bytes.toBytes("message"), Bytes.toBytes("body"));
	    s.addColumn(Bytes.toBytes("message"), Bytes.toBytes("date"));
	    s.addColumn(Bytes.toBytes("message"), Bytes.toBytes("time"));
	    
	    return s;
	}
	
	// Method to retrieve user messages.
	public static ArrayList<String> rangeScan(HTable table, String userName) {
	    ResultScanner scanner = null;
	    
	    ArrayList<String> messages = new ArrayList<>();
		try {
			scanner = table.getScanner(scannerTable(userName)); // ln 338
		} catch (IOException e) {
		}
	
	   	 try {
		    for (Result rr : scanner) {
		    	byte [] author = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("author"));
		    	byte [] recip = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("recip"));
		    	byte [] title = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("title"));
		    	byte [] body = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("body"));
		    	byte [] date = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("date"));
		    	byte [] time = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("time"));

		    	String sAuthor = Bytes.toString(author);
		    	String sRecip = Bytes.toString(recip);
		    	String sTitle = Bytes.toString(title);	
		    	String sBody = Bytes.toString(body);
		    	String sDate = Bytes.toString(date);
		    	String sTime = Bytes.toString(time);
		    	
			String messageFrom = "Message from: " + sAuthor + " \nTitle: " + sTitle 
					+ " \nMessage body: " + sBody + " \nsent: " + sDate + " at " + sTime + "\n\n";
				
			messages.add(messageFrom); // put into an ArrayList for printing to GUI
		    }
	    	} 
	    	finally {
	      	scanner.close();
	    	}
		return messages;
	}
}

