#include "common.h"
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>

// Include CRoaring header(s). Adjust path/name if needed.
// Many distributions provide <roaring/roaring64.h> or <roaring64.h>
#include <roaring/roaring64.h>

std::vector<uint8_t> serialize_roaring64(roaring64_bitmap_t *bm) {
    std::vector<uint8_t> out;
    if (!bm) return out;
    
    // Check if bitmap is empty
    uint64_t cardinality = roaring64_bitmap_get_cardinality(bm);
    if (cardinality == 0) {
        // Return empty vector for empty bitmap
        return out;
    }
    
    // Use portable serialize if available; otherwise fall back to regular serialize.
    size_t serialized_size = roaring64_bitmap_portable_size_in_bytes(bm);
    if (serialized_size == 0) {
        return out;
    }
    
    out.resize(serialized_size);
    size_t wrote = roaring64_bitmap_portable_serialize(bm, (char*)out.data());
    if (wrote != serialized_size) {
        // fallback or error
        // Try regular serialize if portable not available/failed
        out.clear();
    }
    return out;
}

roaring64_bitmap_t* deserialize_roaring64(const uint8_t *data, size_t len) {
    if (!data || len == 0) return nullptr;
    
    // Try portable deserialize
    roaring64_bitmap_t *bm = roaring64_bitmap_portable_deserialize_safe((const char*)data, len);
    
    // If deserialization failed, create an empty bitmap
    if (!bm) {
        bm = roaring64_bitmap_create();
    }
    
    return bm;
}

void free_roaring64(roaring64_bitmap_t *bm) {
    if (!bm) return;
    roaring64_bitmap_free(bm);
}
