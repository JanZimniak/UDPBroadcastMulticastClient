package projekt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UDPClient {

    private DatagramChannel multicastChannel;
    private DatagramChannel broadcastChannel;
    private NetworkInterface nic = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());

    private ExecutorService IOHandling = Executors.newFixedThreadPool(2);

    private final int PORT;
    private final int MIN_PORT_NUMBER = 1024;
    private final int MAX_PORT_NUMBER = 65535;

    private final int BROADCAST_PORT;

    private final int MULTICAST_PORT;
    
    private HashMap<String, MembershipKey> multicastAddresses = new HashMap<>();
    private final String DEFAULT_MULTICAST_ADDRESS = "239.1.1.1";
    
    private volatile boolean isRunning;

    public UDPClient(int host_port) throws IOException{
        this.PORT = host_port;
        this.BROADCAST_PORT = getBroadcastPort(this.PORT);
        this.MULTICAST_PORT = getMulticastPort(this.PORT);
        setupMulticast();
        setupBroadcast();
        this.isRunning = false;
    }

    public void start() {
        this.isRunning = true;
        this.IOHandling.submit(this::handleSend);
        this.IOHandling.submit(this::handleReceive);
        System.out.println("Client is running");
        System.out.println("Type <end> to close client");
        System.out.println("Message structure: <port><type> message");
    }
    
    private void setupMulticast() throws IOException {
        this.multicastChannel = DatagramChannel.open();
        this.multicastChannel.configureBlocking(false);
        this.multicastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.multicastChannel.bind(new InetSocketAddress(this.MULTICAST_PORT));
    }

    private void setupBroadcast() throws IOException {
        this.broadcastChannel = DatagramChannel.open();
        this.broadcastChannel.configureBlocking(false);
        this.broadcastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.broadcastChannel.setOption(StandardSocketOptions.SO_BROADCAST, true);
        this.broadcastChannel.bind(new InetSocketAddress(this.BROADCAST_PORT));
    }

    private void handleReceive(){
        try(Selector selector = Selector.open()){
            this.multicastChannel.register(selector, SelectionKey.OP_READ, "MULTICAST");
            this.broadcastChannel.register(selector, SelectionKey.OP_READ, "BROADCAST");
            
            ByteBuffer buffer = ByteBuffer.allocate(4096);

            while(this.isRunning){
                selector.select();
                for(SelectionKey key : selector.selectedKeys()){
                    buffer.clear();
                    String type = (String)key.attachment();
                    DatagramChannel channel = (DatagramChannel)key.channel();

                    InetSocketAddress senderAddress = (InetSocketAddress) channel.receive(buffer);
                    buffer.flip();
                    String message = new String(buffer.array(), 0, buffer.limit()); 
                    System.out.println(type + senderAddress + ": " + message);
                }
                selector.selectedKeys().clear();
            }

        }catch(Exception e){
            if(this.isRunning){
                e.printStackTrace();
            }
        }
    }

    private void handleSend(){
        try(Scanner scanner = new Scanner(System.in)){
            while(this.isRunning){
                System.out.println("Choose type (broadcast/multicast/join/leave/end): ");
                String choice = scanner.nextLine();

                switch(choice.toLowerCase()){
                    case "end" -> {
                        close();
                        return;
                    }
                    case "broadcast" -> {
                        handleBroadcast(scanner); 
                    }
                    case "multicast" -> {
                        handleMulticast(scanner);
                    }
                    case "join" -> {
                        handleJoinOrLeaveMulticastGroup(scanner, true);
                    }
                    case "leave" -> {
                        handleJoinOrLeaveMulticastGroup(scanner, false);
                    }
                    default -> System.out.println("Unknown option.");
                }
            }
        } catch(Exception e){
            if(this.isRunning) e.printStackTrace();
        }
    }    

    private void handleBroadcast(Scanner scanner) throws IOException{
        System.out.println("Port: ");
        String portInput = scanner.nextLine();
        if(!checkPort(portInput)){
            System.out.printf("Wrong port number, choose from: %d - %d\n", this.MIN_PORT_NUMBER, this.MAX_PORT_NUMBER);
            return;
        }
        int port = Integer.parseInt(portInput);

        System.out.println("Message: ");
        String message = scanner.nextLine();

        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
        this.broadcastChannel.send(buffer,
            new InetSocketAddress("255.255.255.255", getBroadcastPort(port)));
    }

    private void handleMulticast(Scanner scanner) throws IOException{
        System.out.println("Port: ");

        String portInput = scanner.nextLine();
        if(!checkPort(portInput)){
            System.out.printf("Wrong port number, choose from: %d - %d\n", this.MIN_PORT_NUMBER, this.MAX_PORT_NUMBER);
            return;
        }
        int port = Integer.parseInt(portInput);

        System.out.println("Multicast address (if left empty, default will be used: 239.1.1.1): ");

        if(!this.multicastAddresses.isEmpty()){
            System.out.println("Joined multicast groups:");
            for(String address : this.multicastAddresses.keySet()){
                System.out.println(address);
            }
        }

        String input = scanner.nextLine().trim();
        String address = "";
        if(checkMulticastAddress(input)){
            address = input;
        }
        if(address.isEmpty()){
            System.out.println("Using default multicast address");
            address = this.DEFAULT_MULTICAST_ADDRESS;
        }

        System.out.println("Message: ");
        String message = scanner.nextLine();

        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
        this.multicastChannel.send(buffer,
            new InetSocketAddress(address, getMulticastPort(port)));
    }

    private void handleJoinOrLeaveMulticastGroup(Scanner scanner, boolean isJoining){
        System.out.println("Multicast group IP Address:");
        String ipAddress = scanner.nextLine();
        if(!checkMulticastAddress(ipAddress)){
            System.out.println("Not a multicast IP Address");
            return;
        }
        if(isJoining){
            joinMulticastGroup(ipAddress);
        }else{
            leaveMulticastGroup(ipAddress);
        }
    }

    private void joinMulticastGroup(String ipAddress){
        try{
            MembershipKey key = this.multicastChannel.join(InetAddress.getByName(ipAddress), this.nic);
            this.multicastAddresses.put(ipAddress, key);
        }catch(IOException e){
            System.out.println("Couldn't join multicast group: " + e.getMessage());
        }
    }

    private void leaveMulticastGroup(String ipAddress){
        MembershipKey key = this.multicastAddresses.get(ipAddress);
        key.drop();
        this.multicastAddresses.remove(ipAddress);
    }

    private boolean checkMulticastAddress(String input){
        try{
            return InetAddress.getByName(input).isMulticastAddress();
        }catch(UnknownHostException e){
            return false;
        }
    }

    private boolean checkPort(String portInput){
        int port;
        try{
            port = Integer.parseInt(portInput);
            return this.MIN_PORT_NUMBER <= port && port <= this.MAX_PORT_NUMBER;
        }catch(NumberFormatException e){
            return false;
        }
    }

    private int getMulticastPort(int port){
        return port;
    }

    private int getBroadcastPort(int port){
        return port + 1;
    }

    public void close() throws IOException {
        this.isRunning = false;
        this.IOHandling.shutdownNow();
        this.multicastChannel.close();
        this.broadcastChannel.close();
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Wrong number of arguments: <host_port>");
            return;
        }
        UDPClient client = new UDPClient(Integer.parseInt(args[0]));
        client.start();
    }
}
