package projekt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import projekt.enums.ChannelType;

public class UDPClient {

    private DatagramChannel multicastChannel;
    private DatagramChannel broadcastChannel;
    private NetworkInterface nic = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());

    private ExecutorService IOHandling = Executors.newFixedThreadPool(2);

    private final int PORT;
    private final String BROADCAST_PREFIX = "B";
    private final int BROADCAST_PORT;

    private final String MULTICAST_PREFIX = "M";
    private final int MULTICAST_PORT;
    private final String MULTICAST_GROUP_ADDRESS = "239.1.1.1";
    
    private volatile boolean isRunning;

    public UDPClient(int host_port) throws IOException {
        this.PORT = host_port;
        this.BROADCAST_PORT = getBroadcastPort(this.PORT);
        this.MULTICAST_PORT = getMulticastPort(this.PORT);

        this.isRunning = false;
        setupMulticast();
        setupBroadcast();
    }

    private void setupMulticast() throws IOException {
        this.multicastChannel = DatagramChannel.open();
        this.multicastChannel.configureBlocking(false);
        this.multicastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.multicastChannel.bind(new InetSocketAddress(this.MULTICAST_PORT));
        this.multicastChannel.join(InetAddress.getByName(this.MULTICAST_GROUP_ADDRESS), this.nic);
    }

    private void setupBroadcast() throws IOException {
        this.broadcastChannel = DatagramChannel.open();
        this.broadcastChannel.configureBlocking(false);
        this.broadcastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.broadcastChannel.setOption(StandardSocketOptions.SO_BROADCAST, true);
        this.broadcastChannel.bind(new InetSocketAddress(this.BROADCAST_PORT));
    }

    public void start(){
        this.isRunning = true;
        this.IOHandling.submit(this::handleSend);
        this.IOHandling.submit(this::handleReceive);
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
                String input = scanner.nextLine();

                Integer port = getPortFromInput(input);
                ChannelType channelType = getChannelTypeFromInput(input);
                if(channelType == ChannelType.UNDEFINED || port <= 0){
                    System.out.println("Define channel type: <port><channel_type> message");
                    continue;
                }

                String message = cleanInput(input);
                ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));

                if(channelType == ChannelType.MULTICAST){
                    this.multicastChannel.send(buffer, new InetSocketAddress(this.MULTICAST_GROUP_ADDRESS, getMulticastPort(port)));
                }else{
                    this.broadcastChannel.send(buffer, new InetSocketAddress("255.255.255.255", getBroadcastPort(port)));
                }
            }
        }catch(Exception e){
            if(this.isRunning){
                e.printStackTrace();
            }
        
        }
    }

    private int getMulticastPort(int port){
        return port;
    }

    private int getBroadcastPort(int port){
        return port + 1;
    }

    private String cleanInput(String input){
        int prefixLength = 4 + this.MULTICAST_PREFIX.length();
        input = input.substring(prefixLength); 
        if(input.startsWith(" ")){
            input = input.substring(1);
        }
        return input;
    }

    private Integer getPortFromInput(String input){
        String strPort = input.substring(0, 4);
        try{
            int port = Integer.parseInt(strPort);
            return port;
        }catch(NumberFormatException e){
            return -1;
        }
    }

    private ChannelType getChannelTypeFromInput(String input){
        input = input.substring(4);
        if(input.toLowerCase().startsWith(this.BROADCAST_PREFIX.toLowerCase())){
            return ChannelType.BROADCAST;
        }
        if(input.toLowerCase().startsWith(this.MULTICAST_PREFIX.toLowerCase())){
            return ChannelType.MULTICAST;
        }
        return ChannelType.UNDEFINED;
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
