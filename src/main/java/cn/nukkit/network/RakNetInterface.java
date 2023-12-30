package cn.nukkit.network;

import cn.nukkit.Player;
import cn.nukkit.Server;
import cn.nukkit.event.player.PlayerCreationEvent;
import cn.nukkit.event.server.QueryRegenerateEvent;
import cn.nukkit.network.protocol.BatchPacket;
import cn.nukkit.network.protocol.DataPacket;
import cn.nukkit.network.protocol.ProtocolInfo;
import cn.nukkit.network.protocol.ServerToClientHandshakePacket;
import cn.nukkit.utils.Binary;
import cn.nukkit.utils.BinaryStream;
import cn.nukkit.utils.EncryptionUtils;
import cn.nukkit.utils.Utils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nukkitx.natives.sha256.Sha256;
import com.nukkitx.natives.util.Natives;
import com.nukkitx.network.raknet.*;
import com.nukkitx.network.util.DisconnectReason;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.message.FormattedMessage;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * author: MagicDroidX
 * Nukkit Project
 */
@Log4j2
public class RakNetInterface implements RakNetServerListener, AdvancedSourceInterface {

    private final Server server;

    private Network network;

    private final RakNetServer raknet;

    private final Map<InetSocketAddress, NukkitRakNetSession> sessions = new HashMap<>();

    private final Queue<NukkitRakNetSession> sessionCreationQueue = PlatformDependent.newMpscQueue();


    private final Set<ScheduledFuture<?>> tickFutures = new HashSet<>();

    private final FastThreadLocal<Set<NukkitRakNetSession>> sessionsToTick = new FastThreadLocal<Set<NukkitRakNetSession>>() {
        @Override
        protected Set<NukkitRakNetSession> initialValue() {
            return Collections.newSetFromMap(new IdentityHashMap<>());
        }
    };

    private byte[] advertisement;

    private static final KeyPair SERVER_KEY_PAIR = EncryptionUtils.createKeyPair();

    private static final ThreadLocal<Sha256> HASH_LOCAL = ThreadLocal.withInitial(Natives.SHA_256);
    private static final ThreadLocal<byte[]> CHECKSUM_LOCAL = ThreadLocal.withInitial(() -> new byte[8]);

    public RakNetInterface(Server server) {
        this.server = server;

        InetSocketAddress bindAddress = new InetSocketAddress(Strings.isNullOrEmpty(this.server.getIp()) ? "0.0.0.0" : this.server.getIp(), this.server.getPort());

        this.raknet = new RakNetServer(bindAddress, Runtime.getRuntime().availableProcessors());
        this.raknet.bind().join();
        this.raknet.setListener(this);

        for (EventExecutor executor : this.raknet.getBootstrap().config().group()) {
            this.tickFutures.add(executor.scheduleAtFixedRate(() -> {
                for (NukkitRakNetSession session : sessionsToTick.get()) {
                    session.sendOutbound();
                }
            }, 0, 50, TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public void setNetwork(Network network) {
        this.network = network;
    }

    @Override
    public boolean process() {
        NukkitRakNetSession session;
        while ((session = this.sessionCreationQueue.poll()) != null) {
            InetSocketAddress address = session.raknet.getAddress();
            PlayerCreationEvent ev = new PlayerCreationEvent(this, Player.class, Player.class, null, address);
            this.server.getPluginManager().callEvent(ev);
            Class<? extends Player> clazz = ev.getPlayerClass();

            try {
                Constructor<? extends Player> constructor = clazz.getConstructor(SourceInterface.class, Long.class, InetSocketAddress.class);
                Player player = constructor.newInstance(this, ev.getClientId(), ev.getSocketAddress());
                this.server.addPlayer(address, player);
                session.player = player;
                this.sessions.put(address, session);
            } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
                Server.getInstance().getLogger().logException(e);
            }
        }

        Iterator<NukkitRakNetSession> iterator = this.sessions.values().iterator();
        while (iterator.hasNext()) {
            NukkitRakNetSession nukkitSession = iterator.next();
            Player player = nukkitSession.player;
            if (nukkitSession.disconnectReason != null) {
                player.close(player.getLeaveMessage(), nukkitSession.disconnectReason, false);
                iterator.remove();
                continue;
            }
            DataPacket packet;
            while ((packet = nukkitSession.inbound.poll()) != null) {
                try {
                    nukkitSession.player.handleDataPacket(packet);
                } catch (Exception e) {
                    log.error(new FormattedMessage("An error occurred whilst handling {} for {}",
                            new Object[]{packet.getClass().getSimpleName(), nukkitSession.player.getName()}, e));
                }
            }
        }
        return true;
    }

    @Override
    public int getNetworkLatency(Player player) {
        RakNetServerSession session = this.raknet.getSession(player.getSocketAddress());
        return session == null ? -1 : (int) session.getPing();
    }

    @Override
    public void close(Player player) {
        this.close(player, "unknown reason");
    }

    @Override
    public void close(Player player, String reason) {
        RakNetServerSession session = this.raknet.getSession(player.getSocketAddress());
        if (session != null) {
            session.close();
        }
    }

    @Override
    public void shutdown() {
        this.tickFutures.forEach(future -> future.cancel(false));
        this.raknet.close();
    }

    @Override
    public void emergencyShutdown() {
        this.tickFutures.forEach(future -> future.cancel(true));
        this.raknet.close();
    }

    @Override
    public void blockAddress(InetAddress address) {
        this.raknet.block(address);
    }

    @Override
    public void blockAddress(InetAddress address, int timeout) {
        this.raknet.block(address, timeout, TimeUnit.SECONDS);
    }

    @Override
    public void unblockAddress(InetAddress address) {
        this.raknet.unblock(address);
    }

    @Override
    public void sendRawPacket(InetSocketAddress socketAddress, ByteBuf payload) {
        this.raknet.send(socketAddress, payload);
    }

    @Override
    public void setName(String name) {
        QueryRegenerateEvent info = this.server.getQueryInformation();
        String[] names = name.split("!@#");  //Split double names within the program
        String motd = Utils.rtrim(names[0].replace(";", "\\;"), '\\');
        String subMotd = names.length > 1 ? Utils.rtrim(names[1].replace(";", "\\;"), '\\') : "";
        StringJoiner joiner = new StringJoiner(";")
                .add("MCPE")
                .add(motd)
                .add(Integer.toString(ProtocolInfo.CURRENT_PROTOCOL))
                .add(ProtocolInfo.MINECRAFT_VERSION_NETWORK)
                .add(Integer.toString(info.getPlayerCount()))
                .add(Integer.toString(info.getMaxPlayerCount()))
                .add(Long.toString(this.raknet.getGuid()))
                .add(subMotd)
                .add(Server.getGamemodeString(this.server.getDefaultGamemode(), true))
                .add("1");

        this.advertisement = joiner.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Integer putPacket(Player player, DataPacket packet) {
        return this.putPacket(player, packet, false);
    }

    @Override
    public Integer putPacket(Player player, DataPacket packet, boolean needACK) {
        return this.putPacket(player, packet, needACK, false);
    }

    @Override
    public Integer putPacket(Player player, DataPacket packet, boolean needACK, boolean immediate) {
        NukkitRakNetSession session = this.sessions.get(player.getSocketAddress());

        if (session != null) {
            packet.tryEncode();
            session.outbound.offer(packet);
        }

        return null;
    }

    @Override
    public boolean onConnectionRequest(InetSocketAddress inetSocketAddress) {
        return true;
    }

    @Override
    public byte[] onQuery(InetSocketAddress inetSocketAddress) {
        return this.advertisement;
    }

    @Override
    public void onSessionCreation(RakNetServerSession session) {
        NukkitRakNetSession nukkitSession = new NukkitRakNetSession(session);
        session.setListener(nukkitSession);
        this.sessionCreationQueue.offer(nukkitSession);

        // We need to make sure this gets put into the correct thread local hashmap
        // for ticking or race conditions will occur.
        session.getEventLoop().execute(() -> {
            this.sessionsToTick.get().add(nukkitSession);
        });
    }

    @Override
    public void onUnhandledDatagram(ChannelHandlerContext ctx, DatagramPacket datagramPacket) {
        this.server.handlePacket(datagramPacket.sender(), datagramPacket.content());
    }

    public void enableEncryption(Player player) {
        NukkitRakNetSession session = this.sessions.get(player.getSocketAddress());
        if (session != null) {
            session.enableEncryption(player.getLoginChainData().getIdentityPublicKey());
        }
    }

    @RequiredArgsConstructor
    private class NukkitRakNetSession implements RakNetSessionListener {
        private final RakNetServerSession raknet;
        private final Queue<DataPacket> inbound = PlatformDependent.newSpscQueue();
        private final Queue<DataPacket> outbound = PlatformDependent.newMpscQueue();
        private String disconnectReason = null;
        private Player player;

        private volatile SecretKey secretKey;
        private volatile Cipher encryptCipher;
        private volatile Cipher decryptCipher;
        private final AtomicLong encryptCounter = new AtomicLong();
        private final AtomicLong decryptCounter = new AtomicLong();

        @Override
        public void onSessionChangeState(RakNetState rakNetState) {
        }

        @Override
        public void onDisconnect(DisconnectReason disconnectReason) {
            if (disconnectReason == DisconnectReason.TIMED_OUT) {
                this.disconnect("Timed out");
            } else {
                this.disconnect("Disconnected from Server");
            }

            SecretKey secretKey = this.secretKey;
            if (secretKey != null && !secretKey.isDestroyed()) {
                try {
                    secretKey.destroy();
                } catch (DestroyFailedException ignored) {
                }
            }
        }

        @Override
        public void onEncapsulated(EncapsulatedPacket packet) {
            ByteBuf buffer = packet.getBuffer();
            short packetId = buffer.readUnsignedByte();
            if (packetId == 0xfe && buffer.isReadable()) {
                Cipher decryptCipher = this.decryptCipher;
                if (decryptCipher != null) {
                    // This method only supports contiguous buffers, not composite.
                    ByteBuffer inBuffer = buffer.internalNioBuffer(buffer.readerIndex(), buffer.readableBytes());
                    ByteBuffer outBuffer = inBuffer.duplicate();
                    // Copy-safe so we can use the same buffer.
                    try {
                        decryptCipher.update(inBuffer, outBuffer);
                    } catch (GeneralSecurityException e) {
                        this.disconnect("Bad decrypt");
                        log.debug("Unable to decrypt packet", e);
                        return;
                    }

                    // Verify the checksum
                    buffer.markReaderIndex();
                    int trailerIndex = buffer.writerIndex() - 8;
                    byte[] checksum = CHECKSUM_LOCAL.get();
                    try {
                        buffer.readerIndex(trailerIndex);
                        buffer.readBytes(checksum);
                    } catch (Exception e) {
                        this.disconnect("Bad checksum");
                        log.debug("Unable to verify checksum", e);
                        return;
                    }
                    ByteBuf payload = buffer.slice(1, trailerIndex - 1);
                    long count = this.decryptCounter.getAndIncrement();
                    byte[] expected = this.calculateChecksum(count, payload);
                    for (int i = 0; i < 8; i++) {
                        if (checksum[i] != expected[i]) {
                            this.disconnect("Invalid checksum");
                            log.debug("Encrypted packet {} has invalid checksum (expected {}, got {})",
                                    count, Binary.bytesToHexString(expected), Binary.bytesToHexString(checksum));
                            return;
                        }
                    }
                    buffer.resetReaderIndex();
                }

                if (!buffer.isReadable()) {
                    return;
                }

                byte[] packetBuffer = new byte[buffer.readableBytes()];
                buffer.readBytes(packetBuffer);

                try {
                    RakNetInterface.this.network.processBatch(packetBuffer, this.inbound);
                } catch (ProtocolException e) {
                    this.disconnect("Sent malformed packet");
                    log.error("Unable to process batch packet", e);
                }
            }
        }

        @Override
        public void onDirect(ByteBuf byteBuf) {
            // We don't allow any direct packets so ignore.
        }

        private void disconnect(String message) {
            this.disconnectReason = message;
            RakNetInterface.this.sessionsToTick.get().remove(this);
        }

        private void sendOutbound() {
            List<DataPacket> toBatch = new ObjectArrayList<>();
            DataPacket packet;
            while ((packet = this.outbound.poll()) != null) {
                if (packet.pid() == ProtocolInfo.BATCH_PACKET) {
                    if (!toBatch.isEmpty()) {
                        this.sendPackets(toBatch);
                        toBatch.clear();
                    }

                    this.sendPacket(((BatchPacket) packet).payload);
                } else {
                    toBatch.add(packet);
                }
            }

            if (!toBatch.isEmpty()) {
                this.sendPackets(toBatch);
            }
        }

        private void sendPackets(Collection<DataPacket> packets) {
            this.sendPackets(packets, true);
        }

        private void sendPackets(Collection<DataPacket> packets, boolean encrypt) {
            BinaryStream batched = new BinaryStream();
            for (DataPacket packet : packets) {
                Preconditions.checkArgument(!(packet instanceof BatchPacket), "Cannot batch BatchPacket");
                Preconditions.checkState(packet.isEncoded, "Packet should have already been encoded");
                byte[] buf = packet.getBuffer();
                batched.putUnsignedVarInt(buf.length);
                batched.put(buf);
            }

            try {
                this.sendPacket(Network.deflateRaw(batched.getBuffer(), network.getServer().networkCompressionLevel), encrypt);
            } catch (IOException e) {
                log.error("Unable to compress batched packets", e);
            }
        }

        private void sendPacket(byte[] payload) {
            this.sendPacket(payload, true);
        }

        private void sendPacket(byte[] payload, boolean encrypt) {
            ByteBuf byteBuf = ByteBufAllocator.DEFAULT.ioBuffer(1 + payload.length + 8);
            byteBuf.writeByte(0xfe);
            Cipher encryptCipher = this.encryptCipher;
            if (encryptCipher != null && encrypt) {
                ByteBuf compressed = Unpooled.wrappedBuffer(payload);
                try {
                    ByteBuffer checksum = ByteBuffer.wrap(this.calculateChecksum(this.encryptCounter.getAndIncrement(), compressed));

                    ByteBuffer outBuffer = byteBuf.internalNioBuffer(1, compressed.readableBytes() + 8);
                    ByteBuffer inBuffer = compressed.internalNioBuffer(compressed.readerIndex(), compressed.readableBytes());

                    try {
                        encryptCipher.update(inBuffer, outBuffer);
                        encryptCipher.update(checksum, outBuffer);
                    } catch (GeneralSecurityException e) {
                        throw new RuntimeException("Unable to encrypt packet", e);
                    }
                    byteBuf.writerIndex(byteBuf.writerIndex() + compressed.readableBytes() + 8);
                } finally {
                    compressed.release();
                }
            } else {
                byteBuf.writeBytes(payload);
            }
            this.raknet.send(byteBuf);
        }

        private synchronized void enableEncryption(String clientPublicKey) {
            byte[] token = EncryptionUtils.generateRandomToken();

            JWSObject jwt;
            SecretKey secretKey;
            try {
                jwt = EncryptionUtils.createHandshakeJwt(SERVER_KEY_PAIR, token);
                secretKey = EncryptionUtils.getSecretKey(SERVER_KEY_PAIR.getPrivate(), EncryptionUtils.generateKey(clientPublicKey), token);
            } catch (JOSEException | InvalidKeyException | NoSuchAlgorithmException | InvalidKeySpecException e) {
                throw new RuntimeException(e);
            }

            if (!secretKey.getAlgorithm().equals("AES")) {
                throw new IllegalArgumentException("Invalid key algorithm");
            }
            if (this.encryptCipher != null || this.decryptCipher != null) {
                throw new IllegalStateException("Encryption has already been enabled");
            }

            boolean useGcm = ProtocolInfo.CURRENT_PROTOCOL > 428;
            this.encryptCipher = EncryptionUtils.createCipher(useGcm, true, secretKey);
            this.decryptCipher = EncryptionUtils.createCipher(useGcm, false, secretKey);
            this.secretKey = secretKey;

            ServerToClientHandshakePacket handshake = new ServerToClientHandshakePacket();
            handshake.jwt = jwt.serialize();
            handshake.tryEncode();
            // This is sent in cleartext to complete the Diffie Hellman key exchange.
            this.sendPackets(Collections.singletonList(handshake), false);

            if (log.isTraceEnabled() && !server.isIgnoredPacket(ServerToClientHandshakePacket.class)) {
                log.trace("Outbound {}: {}", this.player.getName(), handshake);
            }
        }

        private byte[] calculateChecksum(long count, ByteBuf payload) {
            Sha256 hash = HASH_LOCAL.get();
            ByteBuf counterBuf = ByteBufAllocator.DEFAULT.directBuffer(8);
            try {
                counterBuf.writeLongLE(count);
                ByteBuffer keyBuffer = ByteBuffer.wrap(this.secretKey.getEncoded());

                hash.update(counterBuf.internalNioBuffer(0, 8));
                hash.update(payload.internalNioBuffer(payload.readerIndex(), payload.readableBytes()));
                hash.update(keyBuffer);
                byte[] digested = hash.digest();
                return Arrays.copyOf(digested, 8);
            } finally {
                counterBuf.release();
                hash.reset();
            }
        }
    }
}
