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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UDPClient {

    private DatagramChannel multicastChannel;
    private DatagramChannel broadcastChannel;
    private NetworkInterface nic = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());

    private ExecutorService IOHandling = Executors.newFixedThreadPool(2);

    private final int PORT;
    private final int BROADCAST_PORT;

    private final int MULTICAST_PORT;
    
    private ArrayList<String> multicastAddresses = new ArrayList<>();
    
    private volatile boolean isRunning;

    public UDPClient(int host_port) throws UnknownHostException, SocketException{
        this.PORT = host_port;
        this.BROADCAST_PORT = getBroadcastPort(this.PORT);
        this.MULTICAST_PORT = getMulticastPort(this.PORT);

        this.isRunning = false;
    }

    public void start() throws IOException{
        setupMulticast();
        setupBroadcast();
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

        for(String address : this.multicastAddresses){
            this.multicastChannel.join(InetAddress.getByName(address), this.nic);
        }
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
                System.out.println("Choose type (broadcast/multicast/end): ");
                String choice = scanner.nextLine();

                switch(choice.toLowerCase()){
                    case "end" -> {
                        close();
                        return;
                    }
                    case "broadcast" -> {
                        System.out.println("Port: ");
                        int port = Integer.parseInt(scanner.nextLine());

                        System.out.println("Message: ");
                        String message = scanner.nextLine();

                        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
                        this.broadcastChannel.send(buffer,
                            new InetSocketAddress("255.255.255.255", getBroadcastPort(port)));
                    }
                    case "multicast" -> {
                        System.out.println("Port: ");
                        int port = Integer.parseInt(scanner.nextLine());

                        System.out.println("Multicast address (if left empty, first from the list will be chosen): ");
                        for(String address : this.multicastAddresses){
                            System.out.println(address);
                        }
                        String input = scanner.nextLine().trim();
                        boolean isAddressCorrect = checkAddress(input);
                        String address;
                        if(isAddressCorrect){
                            address = input;
                        }else{
                            address = "";
                        }
                        if(address.isEmpty()){
                            System.out.println("Using default multicast address");
                            address = this.multicastAddresses.get(0);
                        }

                        System.out.println("Message: ");
                        String message = scanner.nextLine();

                        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
                        this.multicastChannel.send(buffer,
                            new InetSocketAddress(address, getMulticastPort(port)));
                    }
                    default -> System.out.println("Unknown option.");
                }
            }
        } catch(Exception e){
            if(this.isRunning) e.printStackTrace();
        }
    }    

    private boolean checkAddress(String input){
        try{
            return InetAddress.getByName(input).isMulticastAddress();
        }catch(UnknownHostException e){
            return false;
        }
    }

    public void addMulticastAddress(String address){
        this.multicastAddresses.add(address);
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
        client.addMulticastAddress("239.1.1.1");
        client.start();
    }
}
