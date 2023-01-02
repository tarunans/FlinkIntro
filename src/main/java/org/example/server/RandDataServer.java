package org.example.server;

import java.io.IOException;
import java.util.Random;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.Date;

public class RandDataServer {
	public static void main(String[] args) throws IOException{
		ServerSocket listener = new ServerSocket(9090);
		try{
				Socket socket = listener.accept();
				System.out.println("Got new connection: " + socket.toString());
				try {
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					Random rand = new Random();
					Date d = new Date();
					while (true){
						int i = rand.nextInt(100);
						String s = "" + System.currentTimeMillis() + "," + i;	
						System.out.println(s);
						out.println(s);
						Thread.sleep(50);
					}
				} finally{
					socket.close();
				}
		} catch(Exception e ){
			e.printStackTrace();
		} finally{
			listener.close();
		}
	}
}

