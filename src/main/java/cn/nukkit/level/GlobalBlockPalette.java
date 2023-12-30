package cn.nukkit.level;

import cn.nukkit.Server;
import cn.nukkit.block.BlockID;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import lombok.extern.log4j.Log4j2;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class GlobalBlockPalette {
    private static final Int2IntMap legacyToRuntimeId = new Int2IntOpenHashMap();
    private static final Int2IntMap runtimeIdToLegacy = new Int2IntOpenHashMap();
    private static final AtomicInteger runtimeIdAllocator = new AtomicInteger(0);

    static {
        legacyToRuntimeId.defaultReturnValue(-1);
        runtimeIdToLegacy.defaultReturnValue(-1);

        Gson gson = new Gson();
        Type collectionType = new TypeToken<Collection<PaletteEntry>>() {
        }.getType();

        Collection<PaletteEntry> entries;
        try (InputStream stream = Server.class.getClassLoader().getResourceAsStream("runtime_block_states.json")) {
            if (stream == null) {
                throw new AssertionError("Unable to locate runtime_block_states.json");
            }
            try (Reader reader = new InputStreamReader(new ByteArrayInputStream(ByteStreams.toByteArray(stream)), StandardCharsets.UTF_8)) {
                entries = gson.fromJson(reader, collectionType);
            }
        } catch (IOException e) {
            throw new AssertionError("Unable to load block palette", e);
        }

        for (PaletteEntry entry : entries) {
            int runtimeId = runtimeIdAllocator.getAndIncrement();
            int legacyId = (entry.id << 14) | entry.val;
            runtimeIdToLegacy.putIfAbsent(runtimeId, legacyId);
            legacyToRuntimeId.putIfAbsent(legacyId, runtimeId);
        }
    }

    public static int getOrCreateRuntimeId(int id, int meta) {
        int legacyIdNoMeta = id << 14;
        int legacyId = legacyIdNoMeta | meta;
        int runtimeId = legacyToRuntimeId.get(legacyId);
        if (runtimeId == -1) {
            runtimeId = legacyToRuntimeId.get(legacyIdNoMeta);
            if (runtimeId == -1 && id != BlockID.INFO_UPDATE) {
                log.info("Unable to find runtime id for {}", id);
                return getOrCreateRuntimeId(BlockID.INFO_UPDATE, 0);
            } else if (id == BlockID.INFO_UPDATE){
                throw new IllegalStateException("InfoUpdate state is missing!");
            }
        }
        return runtimeId;
    }

    public static int getOrCreateRuntimeId(int legacyId) throws NoSuchElementException {
        return getOrCreateRuntimeId(legacyId >> 4, legacyId & 0xf);
    }

    public static int getLegacyFullId(int runtimeId) {
        return runtimeIdToLegacy.get(runtimeId);
    }

    private static class PaletteEntry {
        String name;
        List<StateEntry> states;
        int val;
        int id;
    }

    private static class StateEntry {
        String name;
        String type;
        Object value;
    }
}
