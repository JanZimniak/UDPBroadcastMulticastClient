package projekt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UDPClient {

    private DatagramChannel multicastChannel;
    private DatagramChannel broadcastChannel;
    private NetworkInterface nic = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());

    private ExecutorService IOHandling = Executors.newFixedThreadPool(2);

    private final int PORT;
    private final String MULTICAST_GROUP_ADDRESS = "239.1.1.1";

    public UDPClient(int port) throws IOException {
        this.PORT = port;
        setupMulticast();
        setupBroadcast();
    }

    private void setupMulticast() throws IOException {
        this.multicastChannel = DatagramChannel.open();
        this.multicastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.multicastChannel.bind(new InetSocketAddress(this.PORT));
        this.multicastChannel.join(InetAddress.getByName(this.MULTICAST_GROUP_ADDRESS), this.nic);
    }

    private void setupBroadcast() throws IOException {
        this.broadcastChannel = DatagramChannel.open();
        this.broadcastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.broadcastChannel.setOption(StandardSocketOptions.SO_BROADCAST, true);
        this.broadcastChannel.bind(new InetSocketAddress(this.PORT));
    }

    public void start(){
        this.IOHandling.submit(this::handleSend);
        this.IOHandling.submit(this::handleReceive);
    }
    
    private void handleReceive(){
    }

    private void handleSend(){
    }
}
