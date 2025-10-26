#include <duckdb.hpp>
#include <cstring>
#include "common.h"

using namespace duckdb;

// Simple test function following the template pattern
static void TestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<string_t>(result);
    
    for (idx_t i = 0; i < count; i++) {
        result_data[i] = StringVector::AddString(result, "Roaring MSISDN Extension Loaded!");
    }
}

extern "C" {

DUCKDB_EXTENSION_API void duckdb_roaring_msisdn_init(duckdb::DatabaseInstance &db) {
    Connection con(db);
    
    // Register a simple test function first
    con.CreateScalarFunction("roaring_test", {}, LogicalType::VARCHAR, TestFunction);
}

DUCKDB_EXTENSION_API const char *duckdb_roaring_msisdn_version() {
    return "0.1";
}

} // extern C
