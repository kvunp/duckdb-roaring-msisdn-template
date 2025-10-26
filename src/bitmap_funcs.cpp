#include "common.h"
#include <duckdb.hpp>
#include <vector>
#include <cstring>

using namespace duckdb;

namespace duckdb {

// bitmap_has(blob, msisdn) -> BOOLEAN
void BitmapHasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &blob_vec = args.data[0];
    auto &msisdn_vec = args.data[1];
    
    BinaryExecutor::Execute<string_t, int64_t, bool>(blob_vec, msisdn_vec, result, args.size(), 
        [&](string_t blob_str, int64_t msisdn_val) -> bool {
            const uint8_t *data = (const uint8_t*)blob_str.GetDataUnsafe();
            size_t len = blob_str.GetSize();
            auto bm = deserialize_roaring64(data, len);
            if (!bm) {
                return false;
            }
            
            uint64_t v = (uint64_t)msisdn_val;
            bool has = roaring64_bitmap_contains(bm, v);
            free_roaring64(bm);
            return has;
        });
}

// bitmap_cardinality(blob) -> BIGINT
void BitmapCardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &blob_vec = args.data[0];
    
    UnaryExecutor::Execute<string_t, int64_t>(blob_vec, result, args.size(), 
        [&](string_t blob_str) -> int64_t {
            const uint8_t *data = (const uint8_t*)blob_str.GetDataUnsafe();
            size_t len = blob_str.GetSize();
            auto bm = deserialize_roaring64(data, len);
            if (!bm) {
                return 0; // Will be set to NULL by DuckDB
            }
            
            uint64_t card = roaring64_bitmap_get_cardinality(bm);
            free_roaring64(bm);
            return (int64_t)card;
        });
}

// bitmap_intersection(blob, blob) -> BLOB
void BitmapIntersectionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &blob1_vec = args.data[0];
    auto &blob2_vec = args.data[1];
    
    BinaryExecutor::Execute<string_t, string_t, string_t>(blob1_vec, blob2_vec, result, args.size(), 
        [&](string_t blob1_str, string_t blob2_str) -> string_t {
            const uint8_t *data1 = (const uint8_t*)blob1_str.GetDataUnsafe();
            size_t len1 = blob1_str.GetSize();
            auto bm1 = deserialize_roaring64(data1, len1);
            
            const uint8_t *data2 = (const uint8_t*)blob2_str.GetDataUnsafe();
            size_t len2 = blob2_str.GetSize();
            auto bm2 = deserialize_roaring64(data2, len2);
            
            if (!bm1 || !bm2) {
                if (bm1) free_roaring64(bm1);
                if (bm2) free_roaring64(bm2);
                return string_t("");
            }
            
            auto result_bm = roaring64_bitmap_copy(bm1);
            roaring64_bitmap_and_inplace(result_bm, bm2);
            
            auto serialized = serialize_roaring64(result_bm);
            free_roaring64(bm1);
            free_roaring64(bm2);
            free_roaring64(result_bm);
            
            if (serialized.empty()) {
                return string_t("");
            }
            
            return string_t((char*)serialized.data(), serialized.size());
        });
}

// bitmap_union(blob, blob) -> BLOB
void BitmapUnionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &blob1_vec = args.data[0];
    auto &blob2_vec = args.data[1];
    
    BinaryExecutor::Execute<string_t, string_t, string_t>(blob1_vec, blob2_vec, result, args.size(), 
        [&](string_t blob1_str, string_t blob2_str) -> string_t {
            const uint8_t *data1 = (const uint8_t*)blob1_str.GetDataUnsafe();
            size_t len1 = blob1_str.GetSize();
            auto bm1 = deserialize_roaring64(data1, len1);
            
            const uint8_t *data2 = (const uint8_t*)blob2_str.GetDataUnsafe();
            size_t len2 = blob2_str.GetSize();
            auto bm2 = deserialize_roaring64(data2, len2);
            
            if (!bm1 || !bm2) {
                if (bm1) free_roaring64(bm1);
                if (bm2) free_roaring64(bm2);
                return string_t("");
            }
            
            auto result_bm = roaring64_bitmap_copy(bm1);
            roaring64_bitmap_or_inplace(result_bm, bm2);
            
            auto serialized = serialize_roaring64(result_bm);
            free_roaring64(bm1);
            free_roaring64(bm2);
            free_roaring64(result_bm);
            
            if (serialized.empty()) {
                return string_t("");
            }
            
            return string_t((char*)serialized.data(), serialized.size());
        });
}

// bitmap_difference(blob, blob) -> BLOB
void BitmapDifferenceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &blob1_vec = args.data[0];
    auto &blob2_vec = args.data[1];
    
    BinaryExecutor::Execute<string_t, string_t, string_t>(blob1_vec, blob2_vec, result, args.size(), 
        [&](string_t blob1_str, string_t blob2_str) -> string_t {
            const uint8_t *data1 = (const uint8_t*)blob1_str.GetDataUnsafe();
            size_t len1 = blob1_str.GetSize();
            auto bm1 = deserialize_roaring64(data1, len1);
            
            const uint8_t *data2 = (const uint8_t*)blob2_str.GetDataUnsafe();
            size_t len2 = blob2_str.GetSize();
            auto bm2 = deserialize_roaring64(data2, len2);
            
            if (!bm1 || !bm2) {
                if (bm1) free_roaring64(bm1);
                if (bm2) free_roaring64(bm2);
                return string_t("");
            }
            
            auto result_bm = roaring64_bitmap_copy(bm1);
            roaring64_bitmap_andnot_inplace(result_bm, bm2);
            
            auto serialized = serialize_roaring64(result_bm);
            free_roaring64(bm1);
            free_roaring64(bm2);
            free_roaring64(result_bm);
            
            if (serialized.empty()) {
                return string_t("");
            }
            
            return string_t((char*)serialized.data(), serialized.size());
        });
}

} // namespace duckdb
