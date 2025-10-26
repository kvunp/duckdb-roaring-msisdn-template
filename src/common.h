#pragma once
#include <vector>
#include <cstdint>
#include <cstddef>

// Forward declare CRoaring 64-bit bitmap type.
typedef struct roaring64_bitmap_s roaring64_bitmap_t;

// CRoaring function declarations
extern "C" {
    roaring64_bitmap_t* roaring64_bitmap_create(void);
    void roaring64_bitmap_free(roaring64_bitmap_t *bm);
    roaring64_bitmap_t* roaring64_bitmap_copy(const roaring64_bitmap_t *bm);
    void roaring64_bitmap_add(roaring64_bitmap_t *bm, uint64_t value);
    bool roaring64_bitmap_contains(const roaring64_bitmap_t *bm, uint64_t value);
    uint64_t roaring64_bitmap_get_cardinality(const roaring64_bitmap_t *bm);
    void roaring64_bitmap_or_inplace(roaring64_bitmap_t *bm1, const roaring64_bitmap_t *bm2);
    void roaring64_bitmap_and_inplace(roaring64_bitmap_t *bm1, const roaring64_bitmap_t *bm2);
    void roaring64_bitmap_andnot_inplace(roaring64_bitmap_t *bm1, const roaring64_bitmap_t *bm2);
    size_t roaring64_bitmap_portable_size_in_bytes(const roaring64_bitmap_t *bm);
    size_t roaring64_bitmap_portable_serialize(const roaring64_bitmap_t *bm, char *buf);
    roaring64_bitmap_t* roaring64_bitmap_portable_deserialize_safe(const char *buf, size_t len);
}

// Serialize roaring64 bitmap to portable bytes.
// Returns an empty vector on error.
std::vector<uint8_t> serialize_roaring64(roaring64_bitmap_t *bm);

// Deserialize bytes into a newly allocated roaring64_bitmap_t*.
// Returns nullptr on error.
roaring64_bitmap_t* deserialize_roaring64(const uint8_t *data, size_t len);

// Free a roaring64 bitmap pointer.
void free_roaring64(roaring64_bitmap_t *bm);
